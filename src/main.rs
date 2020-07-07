extern crate clap;

use actix_web;
use serde::Serialize;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

// DB読み込み先
struct MarketConfig {
    host: String,
    port: String,
    path_db_file: String,
}

// DBからのデータ取得用
#[derive(Serialize)]
struct Ohlc {
    ohlc: Vec<(f64, f64, f64, f64, f64, i64)>,
}

fn main() {
    // コマンドライン引数を取得する
    let args_matches = get_args_matches();

    // 取得するマーケットデータを設定する
    let config = configure_market(args_matches);

    // webサーバーを起動する
    let result = actual_main(config);

    // 終了する
    std::process::exit(result);
}

// コマンドライン引数を取得する
fn get_args_matches() -> clap::ArgMatches<'static> {
    clap::App::new("market-api")
        .version("0.0.1")
        .author("Didy KUPANHY")
        .about("管理API")
        .arg(
            clap::Arg::with_name("host")
                .help("ホストアドレス")
                .short("h")
                .long("host")
                .takes_value(true)
                .default_value("0.0.0.0"),
        )
        .arg(
            clap::Arg::with_name("port")
                .help("ポート番号")
                .short("p")
                .long("port")
                .takes_value(true)
                .default_value("8080"),
        )
        .arg(
            clap::Arg::with_name("path-db-file")
                .help("格納先のDB")
                .long("path-db-file")
                .takes_value(true)
                .required(true),
        )
        .get_matches()
}

// 取得するマーケットデータを設定する
fn configure_market(args_matches: clap::ArgMatches<'static>) -> MarketConfig {
    MarketConfig {
        host: args_matches.value_of("host").unwrap().to_string(),
        port: args_matches.value_of("port").unwrap().to_string(),
        path_db_file: args_matches.value_of("path-db-file").unwrap().to_string(),
    }
}

// webサーバーを起動する
fn actual_main(config: MarketConfig) -> i32 {
    if let Err(err) = run(config) {
        eprintln!("{}", err);
        1
    } else {
        0
    }
}

// APIサーバーを起動する
#[actix_rt::main]
async fn run(config: MarketConfig) -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "info,actix_web=info");
    env_logger::init();

    let addr = format!("{}:{}", config.host, config.port);
    let path_db_file = config.path_db_file;

    let manager = SqliteConnectionManager::file(path_db_file);
    let pool = r2d2::Pool::new(manager).unwrap();

    actix_web::HttpServer::new(move || {
        actix_web::App::new()
            .data(pool.clone())
            .wrap(actix_web::middleware::Logger::default())
            .route(
                "/ohlc/{market}/{pair}/{periods}",
                actix_web::web::get().to(get_ohlc),
            )
    })
    .bind(addr)?
    .run()
    .await
}

fn _get_ohlc(
    conn: &r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>,
    path: &actix_web::web::Path<(String, String, i64)>,
) -> Result<Ohlc, rusqlite::Error> {
    let mut stmt = conn.prepare("SELECT open, high, low, close, volume, unixtime FROM ohlc WHERE market = ?1 and pair = ?2 and periods = ?3")?;

    let rows = stmt.query_map(rusqlite::params![path.0, path.1, path.2], |row| {
        Ok((
            row.get::<_, f64>(0)?,
            row.get::<_, f64>(1)?,
            row.get::<_, f64>(2)?,
            row.get::<_, f64>(3)?,
            row.get::<_, f64>(4)?,
            row.get::<_, i64>(5)?,
        ))
    })?;

    let mut ohlc = Vec::new();
    for row in rows {
        ohlc.push(row?)
    }
    Ok(Ohlc { ohlc: ohlc })
}

async fn get_ohlc(
    path: actix_web::web::Path<(String, String, i64)>,
    db: actix_web::web::Data<Pool<SqliteConnectionManager>>,
) -> Result<actix_web::HttpResponse, actix_web::Error> {
    let res = actix_web::web::block(move || {
        let conn = db.get().unwrap();
        _get_ohlc(&conn, &path)
    })
    .await
    .map(|ohlc| actix_web::HttpResponse::Ok().json(ohlc))
    .map_err(|_| actix_web::HttpResponse::InternalServerError())?;
    Ok(res)
}
