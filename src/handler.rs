use std::sync::Arc;

use crate::{event_not_found, is_sql_action, sqlx_error, SqlAction};

use super::SqliteHandler;
use walle_core::{
    prelude::{async_trait, Action, ActionHandler, Event, GetSelfs, GetStatus, GetVersion},
    resp::{resp_error, Resp},
    structs::Selft,
    EventHandler, OneBot, WalleResult,
};

impl<AH: GetSelfs> GetSelfs for SqliteHandler<AH> {
    fn get_selfs<'life0, 'async_trait>(
        &'life0 self,
    ) -> core::pin::Pin<
        Box<dyn core::future::Future<Output = Vec<Selft>> + core::marker::Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.1.get_selfs()
    }
    fn get_impl<'life0, 'life1, 'async_trait>(
        &'life0 self,
        selft: &'life1 Selft,
    ) -> core::pin::Pin<
        Box<dyn core::future::Future<Output = String> + core::marker::Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.1.get_impl(selft)
    }
}

impl<AH: GetStatus + Sync> GetStatus for SqliteHandler<AH> {
    fn get_status<'life0, 'async_trait>(
        &'life0 self,
    ) -> core::pin::Pin<
        Box<
            dyn core::future::Future<Output = walle_core::structs::Status>
                + core::marker::Send
                + 'async_trait,
        >,
    >
    where
        Self: Sized,
        'life0: 'async_trait,
        Self: core::marker::Sync + 'async_trait,
    {
        self.1.get_status()
    }
    fn is_good<'life0, 'async_trait>(
        &'life0 self,
    ) -> core::pin::Pin<
        Box<dyn core::future::Future<Output = bool> + core::marker::Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.1.is_good()
    }
}

impl<AH: GetVersion> GetVersion for SqliteHandler<AH> {
    fn get_version(&self) -> walle_core::structs::Version {
        self.1.get_version()
    }
}

#[async_trait]
impl<AH0> ActionHandler for SqliteHandler<AH0>
where
    AH0: ActionHandler + Send,
    <AH0 as ActionHandler>::Config: Send,
{
    type Config = AH0::Config;
    fn start<'life0, 'life1, 'async_trait, AH, EH>(
        &'life0 self,
        ob: &'life1 Arc<OneBot<AH, EH>>,
        config: Self::Config,
    ) -> core::pin::Pin<
        Box<
            dyn core::future::Future<Output = WalleResult<Vec<tokio::task::JoinHandle<()>>>>
                + core::marker::Send
                + 'async_trait,
        >,
    >
    where
        AH: ActionHandler<Event, Action, Resp> + Send + Sync + 'static,
        EH: EventHandler<Event, Action, Resp> + Send + Sync + 'static,
        AH: 'async_trait,
        EH: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.1.start(ob, config)
    }
    async fn call<AH, EH>(&self, action: Action, ob: &Arc<OneBot<AH, EH>>) -> WalleResult<Resp>
    where
        AH: ActionHandler + Send + Sync + 'static,
        EH: EventHandler + Send + Sync + 'static,
    {
        if is_sql_action(&action.action) {
            match SqlAction::try_from(action) {
                Ok(action) => {
                    return Ok(match action {
                        SqlAction::GetEvent { id } => match self.0.get_event(&id).await {
                            Ok(Some(e)) => e.into(),
                            Ok(None) => event_not_found("").into(),
                            Err(e) => sqlx_error(e).into(),
                        },
                        SqlAction::GetEvents(g) => match self.0.get_events(g).await {
                            Ok(es) => es.into(),
                            Err(e) => sqlx_error(e).into(),
                        },
                        #[cfg(feature = "delete-events")]
                        SqlAction::DeleteEvents(d) => match self.0.delete_events(d).await {
                            Ok(i) => walle_core::value_map! {
                                "count": i
                            }
                            .into(),
                            Err(e) => sqlx_error(e).into(),
                        },
                    });
                }
                Err(e) => return Ok(resp_error::bad_param(e).into()),
            }
        }
        self.1.call(action, ob).await
    }
    async fn before_call_event<AH, EH>(
        &self,
        event: Event,
        ob: &Arc<OneBot<AH, EH>>,
    ) -> WalleResult<Event>
    where
        AH: ActionHandler + Send + Sync + 'static,
        EH: EventHandler + Send + Sync + 'static,
    {
        if let Err(e) = self.0.insert_event(&event).await {
            tracing::warn!(target: "walle-sqlx", "insert event failed: {}", e);
        }
        self.1.before_call_event(event, ob).await
    }
    fn after_call_event<'life0, 'life1, 'async_trait, AH, EH>(
        &'life0 self,
        ob: &'life1 Arc<OneBot<AH, EH>>,
    ) -> core::pin::Pin<
        Box<dyn core::future::Future<Output = WalleResult<()>> + core::marker::Send + 'async_trait>,
    >
    where
        AH: ActionHandler<Event, Action, Resp> + Send + Sync + 'static,
        EH: EventHandler<Event, Action, Resp> + Send + Sync + 'static,
        AH: 'async_trait,
        EH: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.1.after_call_event(ob)
    }
    fn shutdown<'life0, 'async_trait>(
        &'life0 self,
    ) -> core::pin::Pin<
        Box<dyn core::future::Future<Output = ()> + core::marker::Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.1.shutdown()
    }
}
