use criterion::{Criterion, criterion_group, criterion_main};
use std::time::Duration;

#[cfg(feature = "grpc")]
mod grpc_bench {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::runtime::Runtime;
    use tokio::sync::Semaphore;
    use tonic::{Request, Response, Status};

    pub mod benchmark {
        tonic::include_proto!("benchmark");
    }

    use benchmark::benchmark_service_server::{BenchmarkService, BenchmarkServiceServer};
    use benchmark::{AddRequest, AddResponse, EchoRequest, EchoResponse, GetUserRequest, User};

    #[derive(Debug, Default)]
    pub struct BenchmarkServiceImpl;

    #[tonic::async_trait]
    impl BenchmarkService for BenchmarkServiceImpl {
        async fn echo(
            &self, request: Request<EchoRequest>,
        ) -> Result<Response<EchoResponse>, Status> {
            let req = request.into_inner();
            Ok(Response::new(EchoResponse { message: req.message }))
        }

        async fn add(&self, request: Request<AddRequest>) -> Result<Response<AddResponse>, Status> {
            let req = request.into_inner();
            Ok(Response::new(AddResponse { result: req.a + req.b }))
        }

        async fn get_user(
            &self, request: Request<GetUserRequest>,
        ) -> Result<Response<User>, Status> {
            let req = request.into_inner();
            Ok(Response::new(User {
                id: req.user_id,
                name: format!("User{}", req.user_id),
                email: format!("user{}@example.com", req.user_id),
                age: 25,
                address: "123 Main St".to_string(),
            }))
        }
    }

    pub struct GrpcServer;

    impl GrpcServer {
        pub async fn start(bind_addr: &str) -> (SocketAddr, tokio::sync::oneshot::Sender<()>) {
            let listener = TcpListener::bind(bind_addr).await.expect("Failed to bind");
            let addr = listener.local_addr().unwrap();
            let service = BenchmarkServiceImpl::default();

            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

            tokio::spawn(async move {
                let server = BenchmarkServiceServer::new(service);
                tonic::transport::Server::builder()
                    .add_service(server)
                    .serve_with_incoming_shutdown(
                        tokio_stream::wrappers::TcpListenerStream::new(listener),
                        async {
                            let _ = shutdown_rx.await;
                        },
                    )
                    .await
                    .expect("Server failed");
            });

            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            (addr, shutdown_tx)
        }
    }

    use benchmark::benchmark_service_client::BenchmarkServiceClient;

    pub fn run_grpc_echo_benchmark(
        rt: &Runtime, concurrency: usize, requests_per_client: usize, payload_size: usize,
    ) -> Duration {
        rt.block_on(async {
            let (addr, _shutdown_tx) = GrpcServer::start("127.0.0.1:0").await;
            let endpoint = format!("http://{}", addr);
            let payload = "x".repeat(payload_size);

            // Pre-create all clients
            let mut clients = vec![];
            for _ in 0..concurrency {
                let client = BenchmarkServiceClient::connect(endpoint.clone())
                    .await
                    .expect("Failed to connect");
                clients.push(client);
            }

            // Warmup
            for client in &mut clients {
                for _ in 0..10 {
                    let _ = client.echo(EchoRequest { message: payload.clone() }).await;
                }
            }

            let semaphore = Arc::new(Semaphore::new(concurrency));
            let payload = Arc::new(payload);
            let start = std::time::Instant::now();

            let mut handles = vec![];
            for mut client in clients {
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let payload = payload.clone();

                let handle = tokio::spawn(async move {
                    for _ in 0..requests_per_client {
                        let _ = client.echo(EchoRequest { message: payload.to_string() }).await;
                    }
                    drop(permit);
                });
                handles.push(handle);
            }

            for handle in handles {
                let _ = handle.await;
            }

            start.elapsed()
        })
    }
}

#[cfg(feature = "tokio")]
mod razor_bench {
    use razor_rpc::client::{APIConnPool, APIFact, ClientConfig, endpoint_async, endpoint_client};
    use razor_rpc::error::RpcError;
    use razor_rpc::server::dispatch::Inline;
    use razor_rpc::server::{ServerConfig, ServerDefault, service};
    use razor_rpc_codec::MsgpCodec;
    use razor_rpc_tcp::{TcpClient, TcpServer};
    use razor_stream::server::RpcServer;
    use serde::{Deserialize, Serialize};
    use std::future::Future;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::runtime::Runtime;
    use tokio::sync::Semaphore;

    pub type RT = orb_tokio::TokioRT;
    pub type Codec = MsgpCodec;
    pub type ServerProto = TcpServer<RT>;
    pub type ClientProto = TcpClient<RT>;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct EchoRequest {
        pub message: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    pub struct EchoResponse {
        pub message: String,
    }

    endpoint_client!(BenchmarkClient);

    #[endpoint_async(BenchmarkClient)]
    pub trait BenchmarkService {
        fn echo(
            &self, req: EchoRequest,
        ) -> impl Future<Output = Result<EchoResponse, RpcError<()>>> + Send;
    }

    #[derive(Clone)]
    pub struct BenchmarkServiceImpl;

    #[service]
    impl BenchmarkService for BenchmarkServiceImpl {
        async fn echo(&self, req: EchoRequest) -> Result<EchoResponse, RpcError<()>> {
            Ok(EchoResponse { message: req.message })
        }
    }

    pub type PoolCaller = APIConnPool<Codec, ClientProto>;

    impl BenchmarkClient<PoolCaller> {
        pub fn new_client(config: ClientConfig, addr: &str, rt: &RT) -> Self {
            let facts = APIFact::<Codec>::new(config);
            let pool = facts.new_conn_pool::<ClientProto>(rt, addr);
            BenchmarkClient::new(pool)
        }
    }

    pub async fn start_server(rt: &RT, bind_addr: &str) -> (RpcServer<ServerDefault>, String) {
        let server_config = ServerConfig::default();
        let mut server = RpcServer::new(ServerDefault::new(server_config));
        let service_impl = BenchmarkServiceImpl;
        let dispatch = Inline::<Codec, _>::new(service_impl);
        let actual_addr = server
            .listen::<ServerProto, _>(rt.clone(), bind_addr, dispatch)
            .await
            .expect("server listen");
        (server, actual_addr)
    }

    pub fn run_razor_echo_benchmark(
        rt: &Runtime, concurrency: usize, requests_per_client: usize, payload_size: usize,
    ) -> Duration {
        let rt_ref = RT::new_multi_thread(4);
        rt.block_on(async {
            let (_server, addr) = start_server(&rt_ref, "127.0.0.1:0").await;

            // Warmup
            let client_config = ClientConfig::default();
            let client = BenchmarkClient::new_client(client_config.clone(), &addr, &rt_ref);
            let payload = "x".repeat(payload_size);
            for _ in 0..10 {
                let _ = client.echo(EchoRequest { message: payload.clone() }).await;
            }

            let semaphore = Arc::new(Semaphore::new(concurrency));
            let payload = Arc::new(payload);
            let start = std::time::Instant::now();

            let mut handles = vec![];
            for _ in 0..concurrency {
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let addr = addr.clone();
                let rt_ref = rt_ref.clone();
                let payload = payload.clone();

                let handle = tokio::spawn(async move {
                    let client_config = ClientConfig::default();
                    let client = BenchmarkClient::new_client(client_config, &addr, &rt_ref);
                    for _ in 0..requests_per_client {
                        let _ = client.echo(EchoRequest { message: payload.to_string() }).await;
                    }
                    drop(permit);
                });
                handles.push(handle);
            }

            for handle in handles {
                let _ = handle.await;
            }

            start.elapsed()
        })
    }
}

#[cfg(not(any(feature = "grpc", feature = "volo")))]
fn bench_echo_compare(_c: &mut Criterion) {
    // No-op when grpc/volo features are not enabled
}

#[cfg(feature = "volo")]
mod volo_bench {
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::runtime::Runtime;
    use tokio::sync::Semaphore;

    // Include generated code from volo-build
    pub mod benchmark {
        include!(concat!(env!("OUT_DIR"), "/volo_benchmark.rs"));
    }

    use benchmark::volo_benchmark::benchmark::{
        BenchmarkService, BenchmarkServiceClientBuilder, BenchmarkServiceServer, EchoRequest,
        EchoResponse,
    };

    #[derive(Clone)]
    pub struct BenchmarkServiceImpl;

    impl BenchmarkService for BenchmarkServiceImpl {
        async fn echo(
            &self, req: volo_grpc::Request<EchoRequest>,
        ) -> Result<volo_grpc::Response<EchoResponse>, volo_grpc::Status> {
            let resp = EchoResponse { message: req.into_inner().message };
            Ok(volo_grpc::Response::new(resp))
        }

        async fn add(
            &self, _req: volo_grpc::Request<benchmark::volo_benchmark::benchmark::AddRequest>,
        ) -> Result<
            volo_grpc::Response<benchmark::volo_benchmark::benchmark::AddResponse>,
            volo_grpc::Status,
        > {
            unimplemented!()
        }

        async fn get_user(
            &self, _req: volo_grpc::Request<benchmark::volo_benchmark::benchmark::GetUserRequest>,
        ) -> Result<
            volo_grpc::Response<benchmark::volo_benchmark::benchmark::User>,
            volo_grpc::Status,
        > {
            unimplemented!()
        }
    }

    pub async fn start_server() -> (std::net::SocketAddr, tokio::sync::oneshot::Sender<()>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 0));

        let service = BenchmarkServiceImpl;
        let server =
            volo_grpc::server::Server::new().add_service(BenchmarkServiceServer::new(service));

        tokio::spawn(async move {
            let addr = volo::net::Address::from(addr);
            let _ = server
                .run_with_shutdown(addr, async {
                    rx.await.ok();
                    Ok::<(), std::io::Error>(())
                })
                .await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        (addr, tx)
    }

    pub fn run_volo_echo_benchmark(
        rt: &Runtime, concurrency: usize, requests_per_client: usize, payload_size: usize,
    ) -> Duration {
        rt.block_on(async {
            let (addr, _shutdown_tx) = start_server().await;
            let payload = ::pilota::FastStr::from("x".repeat(payload_size));

            // Pre-create all clients
            let mut clients = vec![];
            for _ in 0..concurrency {
                let client = BenchmarkServiceClientBuilder::new("benchmark.BenchmarkService")
                    .address(volo::net::Address::from(addr))
                    .build();
                clients.push(client);
            }

            // Warmup
            for client in &mut clients {
                for _ in 0..10 {
                    let req = EchoRequest { message: payload.clone() };
                    let _ = client.echo(volo_grpc::Request::new(req)).await;
                }
            }

            let semaphore = Arc::new(Semaphore::new(concurrency));
            let payload = Arc::new(payload);
            let start = std::time::Instant::now();

            let mut handles = vec![];
            for mut client in clients {
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let payload = payload.clone();

                let handle = tokio::spawn(async move {
                    for _ in 0..requests_per_client {
                        let req = EchoRequest { message: payload.as_ref().clone() };
                        let _ = client.echo(volo_grpc::Request::new(req)).await;
                    }
                    drop(permit);
                });
                handles.push(handle);
            }

            for handle in handles {
                let _ = handle.await;
            }

            start.elapsed()
        })
    }
}

#[cfg(all(feature = "grpc", feature = "volo"))]
fn bench_echo_compare(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let concurrency = 10;
    let requests_per_client = 100;
    let payload_size = 1024;
    let total_requests = concurrency * requests_per_client;

    let mut group = c.benchmark_group("echo_1kb");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);

    group.bench_function("grpc", |b| {
        b.iter(|| {
            let duration = grpc_bench::run_grpc_echo_benchmark(
                &rt,
                concurrency,
                requests_per_client,
                payload_size,
            );
            duration
        });
    });

    group.bench_function("volo_grpc", |b| {
        b.iter(|| {
            let duration = volo_bench::run_volo_echo_benchmark(
                &rt,
                concurrency,
                requests_per_client,
                payload_size,
            );
            duration
        });
    });

    group.bench_function("razor_rpc", |b| {
        b.iter(|| {
            let duration = razor_bench::run_razor_echo_benchmark(
                &rt,
                concurrency,
                requests_per_client,
                payload_size,
            );
            duration
        });
    });

    group.finish();

    // Print summary
    println!("\n=== Benchmark Summary ===");
    println!("Concurrency: {}", concurrency);
    println!("Requests per client: {}", requests_per_client);
    println!("Total requests: {}", total_requests);
    println!("Payload size: {} bytes", payload_size);
}

#[cfg(not(feature = "tokio"))]
fn bench_echo_compare(_c: &mut Criterion) {
    // No-op when tokio feature is not enabled
}

criterion_group!(benches, bench_echo_compare);
criterion_main!(benches);
