//! The way out of a node's batch-egress network: `fleet-node egress-proxy`, and
//! `fleet-node egress-check`, which shows that the network has no other.
//!
//! A job on that network reaches nothing but this proxy, which opens a TLS
//! tunnel (HTTP `CONNECT`, port 443) to a host of the job's own list and to
//! no other. The list is the batch's `egress` setting. The node's bridge signs
//! it with the node's egress key and gives it to the job as the password of its
//! `HTTPS_PROXY`, so the proxy keeps no state and a job cannot widen its list.

use anyhow::{Context, Result};
use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine,
};
use clap::Parser;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

/// The largest request head a client may send before its tunnel opens.
const MAX_HEAD: usize = 16 << 10;
const HEAD_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Parser)]
#[command(name = "fleet-node egress-proxy", about = "Tunnel batch jobs to the hosts their batch lists")]
struct ProxyArgs {
    #[arg(long, default_value = "0.0.0.0:3128")]
    listen: SocketAddr,
    /// The node's egress key: the bridge signs each job's host list with it.
    #[arg(long)]
    key_file: PathBuf,
}

#[derive(Parser)]
#[command(name = "fleet-node egress-check", about = "Show that this network reaches its proxy and none of the given addresses")]
struct CheckArgs {
    /// The proxy, as HOST:PORT: it must accept a connection.
    #[arg(long)]
    proxy: String,
    /// Addresses of the node and its networks: none may answer, not even with a refusal.
    unreachable: Vec<IpAddr>,
}

/// Runs `fleet-node egress-proxy|egress-check ARGS`; `None` for the node agent's own arguments.
pub async fn command(args: &[String]) -> Option<Result<()>> {
    let rest = || std::iter::once(&args[0]).chain(&args[2..]);
    match args.get(1).map(String::as_str) {
        Some("egress-proxy") => Some(serve(ProxyArgs::parse_from(rest())).await),
        Some("egress-check") => Some(check(CheckArgs::parse_from(rest())).await),
        _ => None,
    }
}

async fn serve(args: ProxyArgs) -> Result<()> {
    let key = std::fs::read(&args.key_file).with_context(|| format!("egress key {}", args.key_file.display()))?;
    anyhow::ensure!(key.len() >= 32, "the egress key is shorter than 32 bytes");
    let proxy = Arc::new(Proxy { key, port: 443, public_only: true });
    let listener = TcpListener::bind(args.listen).await?;
    tracing::info!(listen = %args.listen, "egress proxy listening");
    proxy.run(listener).await
}

pub struct Proxy {
    key: Vec<u8>,
    /// The only port a tunnel opens to.
    port: u16,
    /// Refuse a listed name that resolves to an address of the node or a private network.
    public_only: bool,
}

/// What the client is told instead of a tunnel.
struct Refusal(u16, &'static str);

impl Proxy {
    async fn run(self: Arc<Self>, listener: TcpListener) -> Result<()> {
        loop {
            let (client, peer) = listener.accept().await?;
            let proxy = self.clone();
            tokio::spawn(async move {
                if let Err(error) = proxy.connection(client, peer).await {
                    tracing::debug!(%peer, %error, "egress connection ended");
                }
            });
        }
    }

    async fn connection(&self, mut client: TcpStream, peer: SocketAddr) -> Result<()> {
        let mut head = Vec::new();
        let end = tokio::time::timeout(HEAD_TIMEOUT, async {
            let mut chunk = [0u8; 2048];
            loop {
                if let Some(end) = head.windows(4).position(|w| w == b"\r\n\r\n") {
                    return Ok(end + 4);
                }
                anyhow::ensure!(head.len() < MAX_HEAD, "request head too large");
                let n = client.read(&mut chunk).await?;
                anyhow::ensure!(n > 0, "closed before a request");
                head.extend_from_slice(&chunk[..n]);
            }
        })
        .await??;
        let request = String::from_utf8_lossy(&head[..end]).into_owned();
        let target = request.lines().next().unwrap_or_default().to_owned();
        match self.tunnel(&request).await {
            Ok((host, mut upstream)) => {
                tracing::info!(%peer, %host, "egress allowed");
                client.write_all(b"HTTP/1.1 200 Connection established\r\n\r\n").await?;
                upstream.write_all(&head[end..]).await?;
                tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
            }
            Err(Refusal(status, reason)) => {
                tracing::warn!(%peer, request = %target, status, reason, "egress refused");
                let challenge = if status == 407 { "Proxy-Authenticate: Basic realm=\"fleet-egress\"\r\n" } else { "" };
                let reply = format!(
                    "HTTP/1.1 {status} {reason}\r\n{challenge}Content-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reason}\n",
                    reason.len() + 1
                );
                client.write_all(reply.as_bytes()).await?;
            }
        }
        Ok(())
    }

    /// Opens the tunnel a request asks for, if its job's list has the host.
    async fn tunnel(&self, request: &str) -> std::result::Result<(String, TcpStream), Refusal> {
        let mut lines = request.lines();
        let mut line = lines.next().unwrap_or_default().split(' ');
        let (Some("CONNECT"), Some(target), Some(_), None) = (line.next(), line.next(), line.next(), line.next()) else {
            return Err(Refusal(405, "only CONNECT tunnels to listed hosts"));
        };
        let token = lines
            .filter_map(|l| l.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case("proxy-authorization"))
            .and_then(|(_, value)| value.trim().strip_prefix("Basic "))
            .and_then(|basic| STANDARD.decode(basic.trim()).ok())
            .and_then(|pair| String::from_utf8(pair).ok())
            .and_then(|pair| pair.split_once(':').map(|(_, password)| password.to_owned()))
            .ok_or(Refusal(407, "the job's proxy credentials are required"))?;
        let hosts = self.hosts(&token).ok_or(Refusal(407, "the job's proxy credentials are not this node's"))?;
        let (host, port) = target.rsplit_once(':').ok_or(Refusal(400, "CONNECT takes HOST:PORT"))?;
        let host = host.trim_end_matches('.').to_ascii_lowercase();
        if port.parse() != Ok(self.port) || !hosts.contains(&host) {
            return Err(Refusal(403, "this host is not on the batch's egress list"));
        }
        let addresses = tokio::net::lookup_host((host.as_str(), self.port))
            .await
            .map_err(|_| Refusal(502, "the host does not resolve"))?
            .filter(|a| !self.public_only || is_public(a.ip()))
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            return Err(Refusal(403, "the host resolves to no public address"));
        }
        for address in addresses {
            if let Ok(Ok(upstream)) = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(address)).await {
                return Ok((host, upstream));
            }
        }
        Err(Refusal(502, "the host does not answer"))
    }

    /// The host list a token carries, if this node's key signed it.
    fn hosts(&self, token: &str) -> Option<Vec<String>> {
        let (list, signature) = token.split_once('.')?;
        let (list, signature) = (URL_SAFE_NO_PAD.decode(list).ok()?, URL_SAFE_NO_PAD.decode(signature).ok()?);
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).ok()?;
        mac.update(&list);
        mac.verify_slice(&signature).ok()?;
        Some(String::from_utf8(list).ok()?.strip_prefix("v1\n")?.split(',').map(str::to_owned).collect())
    }
}

/// An address on the internet: not the node's own, a private network's or a reserved one.
fn is_public(ip: IpAddr) -> bool {
    let v4 = |a: Ipv4Addr| {
        let o = a.octets();
        !(a.is_private() || a.is_loopback() || a.is_link_local() || a.is_broadcast() || a.is_documentation()
            || a.is_unspecified() || a.is_multicast()
            || o[0] == 100 && o[1] & 0xc0 == 64          // 100.64.0.0/10, carrier NAT
            || o[0] == 192 && o[1] == 0 && o[2] == 0     // 192.0.0.0/24, protocol assignments
            || o[0] == 198 && o[1] & 0xfe == 18          // 198.18.0.0/15, benchmarking
            || o[0] >= 240)
    };
    match ip {
        IpAddr::V4(a) => v4(a),
        IpAddr::V6(a) => {
            let s = a.segments();
            if let Some(mapped) = a.to_ipv4_mapped() {
                return v4(mapped);
            }
            if s[0] == 0x64 && s[1] == 0xff9b && s[2..6] == [0; 4] {
                // 64:ff9b::/96 carries an IPv4 address.
                return v4(Ipv4Addr::new((s[6] >> 8) as u8, s[6] as u8, (s[7] >> 8) as u8, s[7] as u8));
            }
            !(a.is_loopback() || a.is_unspecified() || a.is_multicast()
                || s[0] & 0xfe00 == 0xfc00                // fc00::/7, unique local
                || s[0] & 0xffc0 == 0xfe80                // fe80::/10, link local
                || s[0] == 0x2001 && s[1] == 0x0db8)      // documentation
        }
    }
}

/// Exits 0 only where the proxy answers and no other address does. An address
/// that refuses a connection is reached all the same, so a refusal fails too.
async fn check(args: CheckArgs) -> Result<()> {
    let wait = Duration::from_secs(3);
    tokio::time::timeout(wait, TcpStream::connect(&args.proxy))
        .await
        .ok()
        .and_then(Result::ok)
        .with_context(|| format!("the proxy {} does not answer on this network", args.proxy))?;
    // A port nothing serves: an answer of any kind comes from the address itself.
    for address in args.unreachable {
        match tokio::time::timeout(wait, TcpStream::connect((address, 9))).await {
            Err(_) => {}
            Ok(Err(error))
                if matches!(error.raw_os_error(), Some(libc::ENETUNREACH | libc::EHOSTUNREACH | libc::EADDRNOTAVAIL)) => {}
            Ok(result) => anyhow::bail!(
                "this network reaches {address} ({}); a job on it would reach the node",
                result.map_or_else(|e| e.to_string(), |_| "connected".into())
            ),
        }
    }
    println!("egress network: the proxy answers and nothing else does");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

    /// The token the bridge gives a job: `fleet_bridge.egress.token`.
    fn token(key: &[u8], hosts: &str) -> String {
        let list = format!("v1\n{hosts}");
        let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
        mac.update(list.as_bytes());
        format!("{}.{}", URL_SAFE_NO_PAD.encode(list), URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
    }

    fn request(target: &str, token: Option<&str>) -> String {
        let auth = token
            .map(|t| format!("Proxy-Authorization: Basic {}\r\n", STANDARD.encode(format!("fleet:{t}"))))
            .unwrap_or_default();
        format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n{auth}\r\n")
    }

    /// A proxy for the given port whose listed names may resolve to loopback, and its address.
    async fn proxy(port: u16) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(Arc::new(Proxy { key: KEY.to_vec(), port, public_only: false }).run(listener));
        address
    }

    async fn ask(proxy: SocketAddr, request: &str) -> (TcpStream, String) {
        let mut client = TcpStream::connect(proxy).await.unwrap();
        client.write_all(request.as_bytes()).await.unwrap();
        let mut reply = Vec::new();
        while !reply.ends_with(b"\r\n\r\n") {
            let mut byte = [0u8; 1];
            assert_eq!(client.read(&mut byte).await.unwrap(), 1, "the proxy closed early");
            reply.push(byte[0]);
        }
        (client, String::from_utf8(reply).unwrap())
    }

    #[tokio::test]
    async fn a_job_reaches_a_host_of_its_list_and_no_other() {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = upstream.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = upstream.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut hello = [0u8; 5];
                    stream.read_exact(&mut hello).await.unwrap();
                    stream.write_all(&hello.map(|b| b.to_ascii_uppercase())).await.unwrap();
                });
            }
        });
        let proxy = proxy(port).await;
        let listed = token(KEY, "pypi.org,localhost");

        // The tunnel carries bytes both ways, whatever the name's case or trailing dot.
        for name in ["localhost", "LocalHost."] {
            let (mut client, reply) = ask(proxy, &request(&format!("{name}:{port}"), Some(&listed))).await;
            assert!(reply.starts_with("HTTP/1.1 200 "), "{reply}");
            client.write_all(b"hello").await.unwrap();
            let mut answer = [0u8; 5];
            client.read_exact(&mut answer).await.unwrap();
            assert_eq!(&answer, b"HELLO");
        }

        for (case, request, status) in [
            // A host the list lacks, a listed host's other port, and an address in place of a name.
            ("unlisted", request(&format!("example.com:{port}"), Some(&listed)), 403),
            ("port", request("localhost:22", Some(&listed)), 403),
            ("address", request(&format!("127.0.0.1:{port}"), Some(&listed)), 403),
            ("suffix", request(&format!("evil-localhost:{port}"), Some(&listed)), 403),
            // No credentials, another node's key, and a list edited after it was signed.
            ("anonymous", request(&format!("localhost:{port}"), None), 407),
            ("foreign", request(&format!("localhost:{port}"), Some(&token(b"another node's 32-byte egress key", "localhost"))), 407),
            ("edited", request(&format!("localhost:{port}"), Some(&listed.replacen(&URL_SAFE_NO_PAD.encode("v1\npypi.org,localhost"), &URL_SAFE_NO_PAD.encode("v1\nexample.com"), 1))), 407),
            // Plain HTTP through the proxy is no tunnel.
            ("plain", format!("GET http://localhost:{port}/ HTTP/1.1\r\nHost: localhost\r\n\r\n"), 405),
        ] {
            let (_, reply) = ask(proxy, &request).await;
            assert!(reply.starts_with(&format!("HTTP/1.1 {status} ")), "{case}: {reply}");
        }
    }

    #[tokio::test]
    async fn a_listed_name_that_resolves_to_the_node_is_refused() {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = upstream.local_addr().unwrap().port();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(Arc::new(Proxy { key: KEY.to_vec(), port, public_only: true }).run(listener));
        let (_, reply) = ask(address, &request(&format!("localhost:{port}"), Some(&token(KEY, "localhost")))).await;
        assert!(reply.starts_with("HTTP/1.1 403 "), "{reply}");
    }

    #[test]
    fn only_internet_addresses_are_public() {
        for public in ["1.1.1.1", "151.101.0.223", "2606:4700::6810:84e5", "100.63.255.255", "100.128.0.1"] {
            assert!(is_public(public.parse().unwrap()), "{public}");
        }
        for private in [
            "127.0.0.1", "10.0.0.1", "172.17.0.1", "192.168.1.1", "169.254.169.254", "100.64.0.1", "0.0.0.0",
            "224.0.0.1", "255.255.255.255", "198.18.0.1", "240.0.0.1", "::1", "::", "fe80::1", "fd00::1",
            "::ffff:10.0.0.1", "::ffff:127.0.0.1", "64:ff9b::a00:1", "ff02::1", "2001:db8::1",
        ] {
            assert!(!is_public(private.parse().unwrap()), "{private}");
        }
    }

    #[tokio::test]
    async fn the_check_passes_only_where_the_proxy_answers_and_nothing_else_does() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = listener.local_addr().unwrap().to_string();
        // Loopback refuses port 9: an answer, so the address is reached.
        let reached = check(CheckArgs { proxy: proxy.clone(), unreachable: vec!["127.0.0.1".parse().unwrap()] }).await;
        assert!(reached.unwrap_err().to_string().contains("reaches 127.0.0.1"));
        check(CheckArgs { proxy, unreachable: vec![] }).await.unwrap();
        // A proxy that is not on the network: its name resolves to nothing.
        let gone = check(CheckArgs { proxy: "fleet-egress-proxy.invalid:3128".into(), unreachable: vec![] }).await;
        assert!(gone.unwrap_err().to_string().contains("does not answer"));
        drop(listener);
    }
}
