use rusqlite::{params, Connection, Result as SqliteResult};
use lazy_static::lazy_static;
use std::sync::Mutex;
use serde_json::json;
use actix_web::{web, App, HttpResponse, HttpServer, Error};
use actix_multipart::Multipart;
use futures_util::TryStreamExt;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write, Read};

lazy_static! {
    static ref DB_CONN: Mutex<Connection> = {
        let conn = Connection::open("haystack.sqlite").expect("Failed to open the database");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS haystack (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT NOT NULL,
                offset INTEGER,
                size INTEGER
            )",
            [],
        ).expect("Failed to create table");
        Mutex::new(conn)
    };
}

fn check_key(key: &str) -> SqliteResult<bool> {
    let conn = DB_CONN.lock().unwrap();
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM haystack WHERE key = ?1")?;
    let count: i64 = stmt.query_row(params![key], |row| row.get(0))?;
    Ok(count > 0)
}

fn upload_sql(key: &str, offset: u64, size: u64) -> SqliteResult<()> {
    let conn = DB_CONN.lock().unwrap();
    conn.execute(
        "INSERT INTO haystack (key, offset, size) VALUES (?1, ?2, ?3)",
        params![key, offset, size],
    )?;
    Ok(())
}

fn get_sql(key: &str) -> SqliteResult<(u64, u64)> {
    let conn = DB_CONN.lock().unwrap();
    let mut stmt = conn.prepare("SELECT offset, size FROM haystack WHERE key = ?1")?;
    let (offset, size): (u64, u64) = stmt.query_row(params![key], |row| {
        Ok((row.get(0)?, row.get(1)?))
    })?;
    Ok((offset, size))
}

#[actix_web::post("/upload/{key}")]
async fn upload_files(key: web::Path<String>, mut payload: Multipart) -> Result<HttpResponse, Error> {
    let key = key.into_inner();
    if check_key(&key).map_err(actix_web::error::ErrorInternalServerError)? {
        return Ok(HttpResponse::BadRequest().body("Key already exists"));
    }

    let mut storage_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("haystack.bin")
        .map_err(actix_web::error::ErrorInternalServerError)?;

    if let Ok(Some(mut field)) = payload.try_next().await {
        let mut file_data = Vec::new();
        while let Some(chunk) = field.try_next().await? {
            file_data.extend_from_slice(&chunk);
        }

        let offset = storage_file.seek(SeekFrom::End(0))
            .map_err(actix_web::error::ErrorInternalServerError)?;
        storage_file.write_all(&file_data)
            .map_err(actix_web::error::ErrorInternalServerError)?;
        let size = file_data.len() as u64;
        upload_sql(&key, offset, size)
            .map_err(actix_web::error::ErrorInternalServerError)?;
        Ok(HttpResponse::Ok().body(format!("File uploaded successfully: key = {}", key)))
    } else {
        Ok(HttpResponse::BadRequest().body("No file was uploaded"))
    }
}

#[actix_web::get("/get/{key}")]
async fn retrieve_file(key: web::Path<String>) -> Result<HttpResponse, Error> {
    let key = key.into_inner();
    if !check_key(&key).map_err(actix_web::error::ErrorInternalServerError)? {
        return Ok(HttpResponse::NotFound().body("Key not found"));
    }
    let (offset, size) = get_sql(&key)
        .map_err(actix_web::error::ErrorInternalServerError)?;
    let mut file = File::open("haystack.bin")
        .map_err(actix_web::error::ErrorInternalServerError)?;
    file.seek(SeekFrom::Start(offset))
        .map_err(actix_web::error::ErrorInternalServerError)?;
    let mut buffer = vec![0; size as usize];
    file.read_exact(&mut buffer)
        .map_err(actix_web::error::ErrorInternalServerError)?;
    Ok(HttpResponse::Ok()
        .content_type("application/octet-stream")
        .append_header((
            "Content-Disposition",
            format!("attachment; filename=\"{}\"", key),
        ))
        .body(buffer))
}

#[actix_web::put("/update/{key}")]
async fn update_file(key: web::Path<String>, mut payload: Multipart) -> Result<HttpResponse, Error> {
    let key = key.into_inner();
    if !check_key(&key).map_err(actix_web::error::ErrorInternalServerError)? {
        return Ok(HttpResponse::NotFound().body("Key not found"));
    }
    let mut storage_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("haystack.bin")
        .map_err(actix_web::error::ErrorInternalServerError)?;

    if let Ok(Some(mut field)) = payload.try_next().await {
        let mut file_data = Vec::new();
        while let Some(chunk) = field.try_next().await? {
            file_data.extend_from_slice(&chunk);
        }

        let offset = storage_file.seek(SeekFrom::End(0))
            .map_err(actix_web::error::ErrorInternalServerError)?;
        storage_file.write_all(&file_data)
            .map_err(actix_web::error::ErrorInternalServerError)?;
        let size = file_data.len() as u64;
        let conn = DB_CONN.lock().unwrap();
        conn.execute(
            "UPDATE haystack SET offset = ?1, size = ?2 WHERE key = ?3",
            params![offset, size, key],
        ).map_err(actix_web::error::ErrorInternalServerError)?;

        Ok(HttpResponse::Ok().body(format!("File updated successfully: key = {}", key)))
    } else {
        Ok(HttpResponse::BadRequest().body("No file was uploaded"))
    }
}

#[actix_web::delete("/delete/{key}")]
async fn delete_file(key: web::Path<String>) -> Result<HttpResponse, Error> {
    let key = key.into_inner();
    if !check_key(&key).map_err(actix_web::error::ErrorInternalServerError)? {
        return Ok(HttpResponse::NotFound().body("Key not found"));
    }
    let (offset, size) = get_sql(&key)
        .map_err(actix_web::error::ErrorInternalServerError)?;
    let mut delete_log = OpenOptions::new()
        .create(true)
        .append(true)
        .open("delete.log")
        .map_err(actix_web::error::ErrorInternalServerError)?;
    let log_entry = json!({
        &key: {
            "offset": offset,
            "size": size
        }
    });
    
    delete_log.seek(SeekFrom::End(0))
        .map_err(actix_web::error::ErrorInternalServerError)?;
    writeln!(delete_log, "{}", log_entry.to_string())
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let conn = DB_CONN.lock().unwrap();
    conn.execute(
        "DELETE FROM haystack WHERE key = ?1",
        params![key],
    ).map_err(actix_web::error::ErrorInternalServerError)?;

    Ok(HttpResponse::Ok().body(format!("File deleted successfully: key = {}", key)))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .service(upload_files)
            .service(retrieve_file)
            .service(update_file)
            .service(delete_file)  
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}