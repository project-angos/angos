use std::collections::HashSet;

use super::{Configuration, Error};

impl Configuration {
    pub fn validate(&self) -> Result<(), Error> {
        for (name, webhook) in &self.auth.webhook {
            webhook.validate().map_err(|e| {
                let msg = format!("Invalid webhook '{name}': {e}");
                Error::InvalidFormat(msg)
            })?;
        }

        let webhook_names = self.auth.webhook.keys().collect::<HashSet<_>>();

        if let Some(webhook_name) = &self.global.authorization_webhook
            && !webhook_names.contains(&webhook_name)
        {
            let msg = format!("Webhook '{webhook_name}' not found (referenced globally)");
            return Err(Error::InvalidFormat(msg));
        }

        for (repository, config) in &self.repository {
            if let Some(webhook_name) = &config.authorization_webhook
                && !webhook_name.is_empty()
                && !webhook_names.contains(&webhook_name)
            {
                let msg = format!(
                    "Webhook '{webhook_name}' not found (referenced in '{repository}' repository)"
                );
                return Err(Error::InvalidFormat(msg));
            }
        }

        let event_webhook_names = self.event_webhook.keys().collect::<HashSet<_>>();

        for (name, config) in &self.event_webhook {
            config.validate().map_err(|e| {
                let msg = format!("Invalid event webhook '{name}': {e}");
                Error::InvalidFormat(msg)
            })?;
        }

        for name in &self.global.event_webhooks {
            if !event_webhook_names.contains(name) {
                let msg = format!("Event webhook '{name}' not found (referenced globally)");
                return Err(Error::InvalidFormat(msg));
            }
        }

        for (repository, config) in &self.repository {
            for name in &config.event_webhooks {
                if !event_webhook_names.contains(name) {
                    let msg = format!(
                        "Event webhook '{name}' not found (referenced in '{repository}' repository)"
                    );
                    return Err(Error::InvalidFormat(msg));
                }
            }
        }

        Ok(())
    }
}
