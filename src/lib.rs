mod handler;

use walle_core::{
    prelude::{Event, TryFromAction, TryFromValue},
    util::ValueMapExt,
};

#[derive(Debug, TryFromAction)]
pub enum SqlAction {
    GetEvent {
        id: String,
    },
    GetEvents(GetEvents),
    #[cfg(feature = "delete-events")]
    DeleteEvents(DeleteEvents),
}

pub(crate) fn is_sql_action(action: &str) -> bool {
    ["get_event", "get_events"].contains(&action)
}

#[derive(Debug, TryFromAction, TryFromValue, Default)]
pub struct GetEvents {
    pub ty: Option<String>,
    pub detail_type: Option<String>,
    pub sub_type: Option<String>,
    pub user_id: Option<String>,
    pub group_id: Option<String>,
    pub channel_id: Option<String>,
    pub guild_id: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

impl GetEvents {
    pub fn is_empty(&self) -> bool {
        self.ty.is_none()
            && self.detail_type.is_none()
            && self.sub_type.is_none()
            && self.user_id.is_none()
            && self.group_id.is_none()
            && self.channel_id.is_none()
            && self.guild_id.is_none()
    }
}

#[derive(Debug, TryFromAction, TryFromValue, Default)]
pub struct DeleteEvents {
    pub id: Option<String>,
    pub ty: Option<String>,
    pub detail_type: Option<String>,
    pub sub_type: Option<String>,
    pub user_id: Option<String>,
    pub group_id: Option<String>,
    pub channel_id: Option<String>,
    pub guild_id: Option<String>,
    pub before: Option<f64>,
}

impl DeleteEvents {
    pub fn is_empty(&self) -> bool {
        self.id.is_none()
            && self.ty.is_none()
            && self.detail_type.is_none()
            && self.sub_type.is_none()
            && self.user_id.is_none()
            && self.group_id.is_none()
            && self.channel_id.is_none()
            && self.guild_id.is_none()
            && self.before.is_none()
    }
}

use sqlx::SqlitePool;

pub struct SqliteHandler<T>(pub SqliteInner, pub T);

#[derive(Debug, Clone)]
pub struct SqliteInner(SqlitePool);

impl SqliteInner {
    pub async fn new(path: &str) -> Result<Self, sqlx::Error> {
        let pool = SqlitePool::connect(&format!("sqlite:{}", path)).await?;
        let (count,): (u32,) = sqlx::query_as(
            "SELECT COUNT(*) FROM sqlite_master WHERE TYPE='table' AND name='events';",
        )
        .fetch_one(&pool)
        .await?;
        if count == 0 {
            sqlx::query(
                "CREATE TABLE events (
                    id int primary key not null,
                    time real not null,
                    type text not null,
                    detail_type text not null,
                    sub_type text not null,
                    user_id text not null,
                    group_id text not null,
                    channel_id text not null,
                    guild_id text not null,
                    data blob not null
                );",
            )
            .execute(&pool)
            .await?;
        }
        Ok(Self(pool))
    }
    pub async fn insert_event(&self, event: &Event) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?);")
            .bind(&event.id)
            .bind(&event.time)
            .bind(&event.ty)
            .bind(&event.detail_type)
            .bind(&event.sub_type)
            .bind(
                &event
                    .extra
                    .get_downcast::<String>("user_id")
                    .unwrap_or_default(),
            )
            .bind(
                &event
                    .extra
                    .get_downcast::<String>("group_id")
                    .unwrap_or_default(),
            )
            .bind(
                &event
                    .extra
                    .get_downcast::<String>("channel_id")
                    .unwrap_or_default(),
            )
            .bind(
                &event
                    .extra
                    .get_downcast::<String>("guild_id")
                    .unwrap_or_default(),
            )
            .bind(rmp_serde::to_vec(&event.extra).unwrap())
            .execute(&self.0)
            .await?;
        Ok(())
    }
    pub async fn get_event(&self, id: &str) -> Result<Option<Event>, sqlx::Error> {
        let event: (String, f64, String, String, String, Vec<u8>) = match sqlx::query_as(
            "SELECT id, time, type, detail_type, sub_type, data FROM events WHERE id=?;",
        )
        .bind(id)
        .fetch_one(&self.0)
        .await
        {
            Ok(e) => e,
            Err(sqlx::Error::RowNotFound) => return Ok(None),
            Err(e) => return Err(e),
        };
        Ok(Some(Event {
            id: event.0,
            time: event.1,
            ty: event.2,
            detail_type: event.3,
            sub_type: event.4,
            extra: rmp_serde::from_slice(&event.5).unwrap(),
        }))
    }
    pub async fn get_events(&self, g: GetEvents) -> Result<Vec<Event>, sqlx::Error> {
        if g.is_empty() {
            return Ok(vec![]);
        }
        let limit = g.limit.unwrap_or(10);
        let mut wheres = vec![];
        let mut values = vec![];
        let mut s = String::from("SELECT id, time, type, detail_type, sub_type, data FROM events");
        s.push_str(" WHERE ");
        macro_rules! if_let {
            ($t: tt) => {
                if let Some($t) = g.$t {
                    wheres.push(stringify!($t=?));
                    values.push($t);
                }
            };
        }
        if let Some(ty) = g.ty {
            wheres.push("type=?");
            values.push(ty);
        }
        if_let!(detail_type);
        if_let!(sub_type);
        if_let!(user_id);
        if_let!(group_id);
        if_let!(channel_id);
        if_let!(guild_id);
        s.push_str(&wheres.join(" AND "));
        s.push_str(" ORDER BY time DESC");
        s.push_str(&format!(" LIMIT {}", limit));
        if let Some(offset) = g.offset {
            s.push_str(&format!(" OFFSET {}", offset));
        }
        s.push(';');
        let events: Vec<(String, f64, String, String, String, Vec<u8>)> = {
            let mut query = sqlx::query_as(&s);
            for value in values {
                query = query.bind(value);
            }
            query.fetch_all(&self.0).await
        }?;
        Ok(events
            .into_iter()
            .map(|event| Event {
                id: event.0,
                time: event.1,
                ty: event.2,
                detail_type: event.3,
                sub_type: event.4,
                extra: rmp_serde::from_slice(&event.5).unwrap(),
            })
            .collect())
    }
    pub async fn delete_events(&self, d: DeleteEvents) -> Result<u32, sqlx::Error> {
        if d.is_empty() {
            return Ok(0);
        }
        let mut wheres = vec![];
        let mut values = vec![];
        let mut before = None;
        let mut s = String::from("DELETE FROM events");
        s.push_str(" WHERE ");
        macro_rules! if_let {
            ($t: tt) => {
                if let Some($t) = d.$t {
                    wheres.push(stringify!($t=?));
                    values.push($t);
                }
            };
        }
        if let Some(ty) = d.ty {
            wheres.push("type=?");
            values.push(ty);
        }
        if_let!(id);
        if_let!(detail_type);
        if_let!(sub_type);
        if_let!(user_id);
        if_let!(group_id);
        if_let!(channel_id);
        if_let!(guild_id);
        if let Some(b) = d.before {
            wheres.push("time<?");
            before = Some(b);
        }
        s.push_str(&wheres.join(" AND "));
        s.push(';');
        let events = {
            let mut query = sqlx::query(&s);
            for value in values {
                query = query.bind(value);
            }
            if let Some(before) = before {
                query = query.bind(before)
            }
            query.execute(&self.0).await
        }?;
        Ok(events.rows_affected() as u32)
    }
}

use walle_core::resp::RespError;
walle_core::error_type!(sqlx_error, 31001, "sqlx error");
walle_core::error_type!(event_not_found, 31002, "event not exists");

#[cfg(test)]
#[tokio::test]
async fn t() {
    if !std::path::Path::new("test.sqlite").exists() {
        drop(tokio::fs::File::create("test.sqlite").await.unwrap());
    }
    let handler = SqliteInner::new("test.sqlite").await.unwrap();
    if handler.get_event("id").await.unwrap().is_none() {
        handler
            .insert_event(&Event {
                id: "id".to_owned(),
                time: 0.0,
                ty: "type".to_owned(),
                detail_type: "detail_type".to_owned(),
                sub_type: "sub_type".to_owned(),
                extra: walle_core::value_map! {
                    "user_id": "user_id"
                },
            })
            .await
            .unwrap();
        handler
            .insert_event(&Event {
                id: "id0".to_owned(),
                time: 1.0,
                ty: "type".to_owned(),
                detail_type: "detail_type".to_owned(),
                sub_type: "sub_type".to_owned(),
                extra: walle_core::value_map! {
                    "user_id": "user_id"
                },
            })
            .await
            .unwrap();
    };
    let es = handler
        .get_events(GetEvents {
            ty: Some("type".to_owned()),
            detail_type: Some("detail_type".to_owned()),
            ..Default::default()
        })
        .await
        .unwrap();
    println!("{:?}", es);
    println!(
        "{:?}",
        handler
            .delete_events(DeleteEvents {
                before: Some(2.0),
                ..Default::default()
            })
            .await
    )
}
