use hbb_common::{log, ResultType};
use serde_json::value::Value;
use sqlx::{
    mysql::MySqlConnectOptions, mysql::MySqlPool, mysql::MySql,
    ConnectOptions, Connection, Error as SqlxError, MySqlConnection, pool::PoolConnection,
};
use std::{ops::DerefMut, str::FromStr};
//use sqlx::postgres::PgPoolOptions;
//use sqlx::mysql::MySqlPoolOptions;

pub(crate) type MapValue = serde_json::map::Map<String, Value>;

pub struct DbPool {
    url: String,
}

#[derive(Clone)]
pub struct Database {
    pool: MySqlPool,
}

#[derive(Default)]
pub struct Peer {
    pub guid: Vec<u8>,
    pub id: String,
    pub uuid: Vec<u8>,
    pub pk: Vec<u8>,
    pub user: Option<Vec<u8>>,
    pub info: String,
    pub status: Option<i64>,
}

impl Database {
    pub async fn new(url: &str) -> ResultType<Database> {
        if !std::path::Path::new(url).exists() {
            std::fs::File::create(url).ok();
        }
        let n: usize = std::env::var("MAX_DATABASE_CONNECTIONS")
            .unwrap_or("1".to_owned())
            .parse()
            .unwrap_or(1);
        log::debug!("MAX_DATABASE_CONNECTIONS={}", n);
        let pool = MySqlPool::connect(
            url
        ).await?;
        let _ = pool.acquire().await?; // test
        let db = Database { pool };
        db.create_tables().await?;
        Ok(db)
    }

    async fn create_tables(&self) -> ResultType<()> {
        Ok(())
    }

    pub async fn get_peer(&self, id: &str) -> ResultType<Option<Peer>> {

        Ok(sqlx::query_as!(
            Peer,
            r#"select guid, id, uuid, pk, user, status, info from peer where id = ?"#,
            id
        )
        .fetch_optional(&self.pool)
        .await?)
    }

    #[inline]
    pub async fn get_conn(&self) -> ResultType<PoolConnection<MySql>> {
        Ok(self.pool.acquire().await?)
    }

    pub async fn update_peer(&self, payload: MapValue, guid: &[u8]) -> ResultType<()> {
        let mut conn = self.get_conn().await?;
        let mut tx = conn.begin().await?;
        if let Some(v) = payload.get("note") {
            let v = get_str(v);
            sqlx::query!("update peer set note = ? where guid = ?", v, guid)
                .execute(&mut tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn insert_peer(
        &self,
        id: &str,
        uuid: &[u8],
        pk: &[u8],
        info: &str,
    ) -> ResultType<Vec<u8>> {
        let guid = uuid::Uuid::new_v4().as_bytes().to_vec();
        sqlx::query!(
            "insert into peer(guid, id, uuid, pk, info) values(?, ?, ?, ?, ?)",
            guid,
            id,
            uuid,
            pk,
            info
        )
        .execute(&self.pool)
        .await?;
        Ok(guid)
    }

    pub async fn update_pk(
        &self,
        guid: &Vec<u8>,
        id: &str,
        pk: &[u8],
        info: &str,
    ) -> ResultType<()> {
        sqlx::query!(
            "update peer set id=?, pk=?, info=? where guid=?",
            id,
            pk,
            info,
            guid
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn exists_relay_allow_list(
        &self,
        own_id: &str,
    ) -> ResultType<bool> {
        let count = sqlx::query_scalar!(
            "select count(*) from allow_relay_list where rust_id=?",
            own_id
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(count > 0)
    }
}

#[cfg(test)]
mod tests {
    use hbb_common::tokio;
    #[test]
    fn test_insert() {
        insert();
    }

    #[test]
    fn test_insert_one() {
        insert_one();
    }

    #[tokio::main(flavor = "multi_thread")]
    async fn insert_one() {
        let db = super::Database::new("mysql://root:123456@localhost/rustdesk").await.unwrap();
        let empty_vec = Vec::new();
        db.insert_peer("1", &empty_vec, &empty_vec, "")
        .await
        .unwrap();
    }

    #[tokio::main(flavor = "multi_thread")]
    async fn insert() {
        let db = super::Database::new("mysql://").await.unwrap();
        let mut jobs = vec![];
        for i in 0..10000 {
            let cloned = db.clone();
            let id = i.to_string();
            let a = tokio::spawn(async move {
                let empty_vec = Vec::new();
                cloned
                    .insert_peer(&id, &empty_vec, &empty_vec, "")
                    .await
                    .unwrap();
            });
            jobs.push(a);
        }
        for i in 0..10000 {
            let cloned = db.clone();
            let id = i.to_string();
            let a = tokio::spawn(async move {
                cloned.get_peer(&id).await.unwrap();
            });
            jobs.push(a);
        }
        hbb_common::futures::future::join_all(jobs).await;
    }
}

pub(crate) fn get_str(v: &Value) -> Option<&str> {
    match v {
        Value::String(v) => {
            let v = v.trim();
            if v.is_empty() {
                None
            } else {
                Some(v)
            }
        }
        _ => None,
    }
}
