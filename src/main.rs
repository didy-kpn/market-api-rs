extern crate clap;

use actix_web;
use serde::Serialize;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

// サーバー設定
struct Config {
    host: String,
    port: String,
    path_db_file: String,
}

// DBの価格データ構造体
#[derive(Serialize)]
struct Ohlc {
    ohlc: Vec<(f64, f64, f64, f64, f64, i64)>,
}

// DBのbotデータ構造体
#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Bot {
    id: i64,
    name: String,
    description: String,
    enable: bool,
    registered: i64,
    token: String,
    long_order: bool,
    short_order: bool,
    operate_type: String,
}

// DBのbotデータ構造体(id,tokenなし)
#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct BotForGet {
    name: String,
    description: String,
    enable: bool,
    registered: i64,
    long_order: bool,
    short_order: bool,
    operate_type: String,
}

#[derive(serde::Deserialize)]
struct GetId {
    id: i64
}

fn main() {
    // コマンドライン引数を取得する
    let args_matches = get_args_matches();

    // サーバー設定情報を取得する
    let config = get_config(args_matches);

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

// APIサーバーの設定を取得する
fn get_config(args_matches: clap::ArgMatches<'static>) -> Config {
    Config {
        host: args_matches.value_of("host").unwrap().to_string(),
        port: args_matches.value_of("port").unwrap().to_string(),
        path_db_file: args_matches.value_of("path-db-file").unwrap().to_string(),
    }
}

// webサーバーを起動する
fn actual_main(config: Config) -> i32 {
    if let Err(err) = run(config) {
        eprintln!("{}", err);
        1
    } else {
        0
    }
}

// APIサーバーを起動する
#[actix_rt::main]
async fn run(config: Config) -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "info,actix_web=info");
    env_logger::init();

    let addr = format!("{}:{}", config.host, config.port);
    let path_db_file = config.path_db_file;

    let manager = SqliteConnectionManager::file(path_db_file);
    let pool = r2d2::Pool::new(manager).unwrap();

    actix_web::HttpServer::new(move || {
        actix_web::App::new()
            .wrap(actix_web::middleware::Logger::default())
            .data(actix_web::web::JsonConfig::default().limit(4096))
            .data(pool.clone())
            .service(actix_web::web::resource("/").route(actix_web::web::get().to(index)))
            .service(
                actix_web::web::resource("/ohlc/{market}/{pair}/{periods}")
                    .route(actix_web::web::get().to(get_ohlc)),
            )
            .service(actix_web::web::resource("/bot").route(actix_web::web::post().to(post_bot)))
            .service(actix_web::web::resource("/bot/{id}").route(actix_web::web::get().to(get_bot)))
    })
    .bind(addr)?
    .run()
    .await
}

fn _get_index() -> Result<String, String> {
    Ok(r#"
        GET /ohlc/{market}/{pair}/{periods}
        GET /bot/{bot-id}
        POST /bot
    "#
    .to_string())
}

async fn index() -> Result<actix_web::HttpResponse, actix_web::Error> {
    let res = actix_web::web::block(move || _get_index())
        .await
        .map(|body| actix_web::HttpResponse::Ok().body(body))
        .map_err(|_| actix_web::HttpResponse::InternalServerError())?;
    Ok(res)
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

fn _post_bot(
    conn: &mut r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>,
    json: serde_json::Value,
) -> Result<Bot, String> {
    let mut option: std::collections::HashMap<String, String> = std::collections::HashMap::new();

    // name チェック
    if json.get("name").is_none()
        || !json["name"].is_string()
        || json["name"].as_str().unwrap().len() == 0
    {
        return Err("name".to_string());
    }
    option.insert(
        "name".to_string(),
        json["name"].as_str().unwrap().to_string(),
    );

    // description チェック
    if json.get("description").is_none()
        || !json["description"].is_string()
        || json["description"].as_str().unwrap().len() == 0
    {
        return Err("description".to_string());
    }
    option.insert(
        "description".to_string(),
        json["description"].as_str().unwrap().to_string(),
    );

    // bool チェック
    if json.get("enable").is_none() || !json["enable"].is_boolean() {
        return Err("enable".to_string());
    }
    option.insert(
        "enable".to_string(),
        if json["enable"].as_bool().unwrap() {
            "1"
        } else {
            "0"
        }
        .to_string(),
    );

    // long_orderが指定されていれば、値を1/0に変換する
    if json.get("long_order").is_some() && json["long_order"].is_boolean() {
        let value = if json["long_order"].as_bool().unwrap() {
            "1"
        } else {
            "0"
        }
        .to_string();
        option.insert("long_order".to_string(), value);
    }

    // short_orderが指定されていれば、値を1/0に変換する
    if json.get("short_order").is_some() && json["short_order"].is_boolean() {
        let value = if json["short_order"].as_bool().unwrap() {
            "1"
        } else {
            "0"
        }
        .to_string();
        option.insert("short_order".to_string(), value);
    }

    // registeredに現在の日時を追加
    {
        let value = chrono::Utc::now().timestamp().to_string();
        option.insert("registered".to_string(), value);
    }

    // 取得したデータをデータベースに保存するためのSQL
    let sql_key = option.keys().map(|s| &**s).collect::<Vec<_>>().join(",");
    let sql_value = option.keys().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql_insert = &format!(
        "INSERT INTO bot ({}, token) VALUES ({}, hex(randomblob(16)))",
        sql_key, sql_value
    );

    let tx = conn.transaction().unwrap();

    // SQLを実行する
    let result = tx.execute(
        sql_insert,
        option.values().map(|s| s.to_string()).collect::<Vec<_>>(),
    );
    let last_id = tx.last_insert_rowid();

    if let Err(err) = result {
        tx.rollback().unwrap();
        return Err(format!("{}", err));
    }

    tx.commit().unwrap();

    // 指定したIDのbot情報を取得する
    let result:Result<Bot, rusqlite::Error> = conn.query_row(
        "select id, name, description, enable, registered, token, long_order, short_order, operate_type from bot where id = ?1",
        rusqlite::params![last_id],
        |row| Ok(Bot {
            id: row.get(0).unwrap(),
            name: row.get(1).unwrap(),
            description: row.get(2).unwrap(),
            enable: row.get(3).unwrap(),
            registered: row.get(4).unwrap(),
            token: row.get(5).unwrap(),
            long_order: row.get(6).unwrap(),
            short_order: row.get(7).unwrap(),
            operate_type: row.get(8).unwrap(),
        })
    );

    if let Err(err) = result {
        return Err(format!("{}", err));
    }

    Ok(result.unwrap())
}

async fn post_bot(
    body: actix_web::web::Bytes,
    db: actix_web::web::Data<Pool<SqliteConnectionManager>>,
) -> Result<actix_web::HttpResponse, actix_web::Error> {
    let result = std::str::from_utf8(&body).unwrap();
    let json: serde_json::Value = serde_json::from_str(result).unwrap();

    let res = actix_web::web::block(move || {
        let mut conn = db.get().unwrap();
        _post_bot(&mut conn, json)
    })
    .await
    .map(|bot| actix_web::HttpResponse::Ok().json(bot))
    .map_err(|_| actix_web::HttpResponse::InternalServerError())?;
    Ok(res)
}

fn _get_bot(
    conn: &r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>,
    id: i64,
    token: String,
) -> Result<BotForGet, String> {
    let result:Result<BotForGet, rusqlite::Error> = conn.query_row(
        "select name, description, enable, registered, long_order, short_order, operate_type from bot where id = ?1 and token = ?2",
        rusqlite::params![id, token],
        |row| Ok(BotForGet {
            name: row.get(0).unwrap(),
            description: row.get(1).unwrap(),
            enable: row.get(2).unwrap(),
            registered: row.get(3).unwrap(),
            long_order: row.get(4).unwrap(),
            short_order: row.get(5).unwrap(),
            operate_type: row.get(6).unwrap(),
        })
    );

    if let Err(err) = result {
        return Err(format!("{}", err));
    }

    Ok(result.unwrap())
}

fn _get_token(req: actix_web::HttpRequest) -> String {
    if req.headers().get("token").is_some() {
        let token_header = req.headers().get("token");
        token_header.unwrap().to_str().unwrap()
    } else {
        ""
    }.to_string()
}

async fn get_bot(
    req: actix_web::HttpRequest,
    path: actix_web::web::Path<GetId>,
    db: actix_web::web::Data<Pool<SqliteConnectionManager>>,
) -> Result<actix_web::HttpResponse, actix_web::Error> {

    let token = _get_token(req);
    let res = actix_web::web::block(move || {
        let conn = db.get().unwrap();

        _get_bot(&conn, path.id, token)
    })
    .await
    .map(|bot| actix_web::HttpResponse::Ok().json(bot))
    .map_err(|_| actix_web::HttpResponse::InternalServerError())?;
    Ok(res)
}
