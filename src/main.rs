use actix_multipart::Multipart;
use actix_web::{get, post, web, App, Error, HttpResponse, HttpServer, Responder};
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

#[derive(Serialize, Deserialize)]
struct IndexEntry(u64, u64);

struct SimpleHaystack {
    storage_file: String,
    index_file: String,
    index: Mutex<HashMap<String, IndexEntry>>,
}

impl SimpleHaystack {
    fn new(storage_file: String, index_file: Option<String>) -> Self {
        let index_file = index_file.unwrap_or_else(|| "haystack_index.json".to_string());
        let index = Mutex::new(Self::load_index(&index_file));
        SimpleHaystack {
            storage_file,
            index_file,
            index,
        }
    }

    fn load_index(index_file: &str) -> HashMap<String, IndexEntry> {
        if Path::new(index_file).exists() {
            let file = File::open(index_file).unwrap();
            serde_json::from_reader(file).unwrap()
        } else {
            HashMap::new()
        }
    }

    fn add_file(&self, file_name: String, data: &[u8]) {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.storage_file)
            .unwrap();
        let offset = file.seek(SeekFrom::End(0)).unwrap();
        file.write_all(data).unwrap();
        let size = data.len() as u64;
        let mut index = self.index.lock().unwrap();
        index.insert(file_name, IndexEntry(offset, size));
    }

    fn save_index(&self) {
        let index = self.index.lock().unwrap();
        let file = File::create(&self.index_file).unwrap();
        serde_json::to_writer(file, &*index).unwrap();
    }
}

#[post("/upload/")]
async fn upload_file(mut payload: Multipart, haystack: web::Data<SimpleHaystack>) -> Result<HttpResponse, Error> {
    while let Ok(Some(mut field)) = payload.try_next().await {
        let content_disposition = field.content_disposition().unwrap().clone();
        
        if let Some(filename) = content_disposition.get_filename() {
            let mut data = Vec::new();
            while let Some(chunk) = field.next().await {
                data.extend_from_slice(&chunk?);
            }
            haystack.add_file(filename.to_string(), &data);
            haystack.save_index();
            return Ok(HttpResponse::Ok().json(serde_json::json!({
                "filename": filename
            })));
        }
    }
    Ok(HttpResponse::BadRequest().body("Invalid upload"))
}

#[get("/files/{filename}")]
async fn get_file(filename: web::Path<String>, haystack: web::Data<SimpleHaystack>) -> impl Responder {
    let index = haystack.index.lock().unwrap();
    if let Some(IndexEntry(offset, size)) = index.get(filename.as_str()) {
        let mut file = File::open(&haystack.storage_file).unwrap();
        file.seek(SeekFrom::Start(*offset)).unwrap();
        let mut buffer = vec![0; *size as usize];
        file.read_exact(&mut buffer).unwrap();
        HttpResponse::Ok()
            .content_type("application/octet-stream")
            .append_header((
                "Content-Disposition",
                format!("attachment; filename={}", filename)
            ))
            .body(buffer)
    } else {
        HttpResponse::NotFound().body("File not found")
    }
}

#[get("/index/")]
async fn get_index(haystack: web::Data<SimpleHaystack>) -> impl Responder {
    let index = haystack.index.lock().unwrap();
    HttpResponse::Ok().json(&*index)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let haystack = web::Data::new(SimpleHaystack::new(
        "haystack_storage.bin".to_string(),
        None,
    ));

    HttpServer::new(move || {
        App::new()
            .app_data(haystack.clone())
            .service(upload_file)
            .service(get_file)
            .service(get_index)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
