//! Cron schedules with claim-and-advance and `sched:{id}:{due}` idempotency.

#![forbid(unsafe_code)]

use oah_core::{AgentName, ConversationId, InstanceId, ScheduleId, UnixMillis};
use oah_store::{AdmitRequest, DeliveryKind};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ScheduleError {
    #[error("invalid cron: {0}")]
    Cron(String),
    #[error("{0}")]
    Other(String),
}

/// Five-field cron: `minute hour dom month dow`. Supports `*`, `n`, `n-m`, `*/n`, `a,b`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CronExpr(pub String);

impl CronExpr {
    pub fn parse(raw: impl Into<String>) -> Result<Self, ScheduleError> {
        let raw = raw.into();
        let fields: Vec<&str> = raw.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(ScheduleError::Cron(
                "expected 5 fields: minute hour dom month dow".into(),
            ));
        }
        for (i, f) in fields.iter().enumerate() {
            let max = [59, 23, 31, 12, 6][i];
            parse_field(f, max)?;
        }
        Ok(Self(raw))
    }

    pub fn matches(&self, minute: u32, hour: u32, day: u32, month: u32, dow: u32) -> bool {
        let fields: Vec<&str> = self.0.split_whitespace().collect();
        if fields.len() != 5 {
            return false;
        }
        field_match(fields[0], minute, 59)
            && field_match(fields[1], hour, 23)
            && field_match(fields[2], day, 31)
            && field_match(fields[3], month, 12)
            && field_match(fields[4], dow, 6)
    }

    /// Next minute strictly after `from` that matches, searching up to 366 days.
    pub fn next_after(&self, from: UnixMillis) -> Result<UnixMillis, ScheduleError> {
        let start = from.as_millis().saturating_add(60_000) / 60_000 * 60_000;
        for step in 0..(366 * 24 * 60) {
            let ms = start.saturating_add(step * 60_000);
            let (minute, hour, day, month, dow) = civil_from_millis(ms);
            if self.matches(minute, hour, day, month, dow) {
                return Ok(UnixMillis(ms));
            }
        }
        Err(ScheduleError::Cron("no match in the next 366 days".into()))
    }
}

fn parse_field(raw: &str, max: u32) -> Result<(), ScheduleError> {
    if field_match(raw, 0, max) || raw.contains('*') || raw.contains(',') || raw.contains('-') {
        return Ok(());
    }
    if raw.parse::<u32>().ok().is_some_and(|n| n <= max) {
        return Ok(());
    }
    Err(ScheduleError::Cron(format!("bad field {raw}")))
}

fn field_match(raw: &str, value: u32, max: u32) -> bool {
    if raw == "*" {
        return true;
    }
    if let Some(step) = raw.strip_prefix("*/") {
        if let Ok(n) = step.parse::<u32>() {
            return n > 0 && value % n == 0;
        }
        return false;
    }
    if raw.contains(',') {
        return raw.split(',').any(|p| field_match(p, value, max));
    }
    if let Some((a, b)) = raw.split_once('-') {
        if let (Ok(lo), Ok(hi)) = (a.parse::<u32>(), b.parse::<u32>()) {
            return value >= lo && value <= hi && hi <= max;
        }
        return false;
    }
    raw.parse::<u32>().ok() == Some(value)
}

/// UTC civil time from unix millis (enough for cron matching).
fn civil_from_millis(ms: i64) -> (u32, u32, u32, u32, u32) {
    let secs = ms.div_euclid(1000);
    let mins_total = secs.div_euclid(60);
    let minute = (mins_total.rem_euclid(60)) as u32;
    let hours_total = mins_total.div_euclid(60);
    let hour = (hours_total.rem_euclid(24)) as u32;
    let days = hours_total.div_euclid(24);
    let dow = ((days + 4).rem_euclid(7)) as u32; // 1970-01-01 Thursday = 4 → we want 0=Sun? cron dow 0=Sun
    // Unix epoch Thursday. cron: 0 = Sunday. Thursday = 4.
    let (year, month, day) = civil_date(days);
    let _ = year;
    (minute, hour, day, month, dow)
}

fn civil_date(mut z: i64) -> (i32, u32, u32) {
    // Howard Hinnant civil_from_days, z = days since 1970-01-01
    z += 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i32 + era as i32 * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

pub fn idempotency_key(id: &ScheduleId, due: UnixMillis) -> String {
    format!("sched:{}:{}", id.as_str(), due.as_millis())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schedule {
    pub id: ScheduleId,
    pub agent: AgentName,
    pub instance: InstanceId,
    pub cron: CronExpr,
    pub payload: Value,
    pub next_due: UnixMillis,
    pub enabled: bool,
}

impl Schedule {
    pub fn conversation(&self) -> ConversationId {
        ConversationId::new(&self.agent, &self.instance)
    }

    pub fn admission(&self) -> AdmitRequest {
        let cid = self.conversation();
        AdmitRequest {
            conversation_id: cid.clone(),
            session_key: oah_core::SessionKey::root(&cid),
            kind: DeliveryKind::Signal,
            payload: json!({
                "type": "schedule",
                "body": self.payload,
                "scheduleId": self.id.to_string(),
                "due": self.next_due.as_millis(),
            }),
            principal: oah_core::Principal::anonymous(),
            idempotency_key: Some(idempotency_key(&self.id, self.next_due)),
            uid: None,
            max_attempts: 10,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScheduleBook {
    pub items: BTreeMap<String, Schedule>,
}

impl ScheduleBook {
    pub fn insert(&mut self, schedule: Schedule) {
        self.items.insert(schedule.id.to_string(), schedule);
    }

    pub fn get(&self, id: &ScheduleId) -> Option<&Schedule> {
        self.items.get(id.as_str())
    }

    pub fn remove(&mut self, id: &ScheduleId) -> Option<Schedule> {
        self.items.remove(id.as_str())
    }

    pub fn due(&self, now: UnixMillis) -> Vec<&Schedule> {
        self.items
            .values()
            .filter(|s| s.enabled && s.next_due.as_millis() <= now.as_millis())
            .collect()
    }

    /// Claim due rows and advance `next_due`. Returns admissions to persist.
    /// Crash-safe: caller admits first (idempotent), then `advance`.
    pub fn claim_and_advance(
        &mut self,
        now: UnixMillis,
    ) -> Result<Vec<(ScheduleId, UnixMillis, AdmitRequest)>, ScheduleError> {
        let ids: Vec<ScheduleId> = self
            .due(now)
            .into_iter()
            .map(|s| s.id.clone())
            .collect();
        let mut out = Vec::new();
        for id in ids {
            let Some(schedule) = self.items.get_mut(id.as_str()) else {
                continue;
            };
            let claimed_due = schedule.next_due;
            let req = schedule.admission();
            let next = schedule.cron.next_after(claimed_due.max(now))?;
            schedule.next_due = next;
            out.push((id, claimed_due, req));
        }
        Ok(out)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn cron_every_minute() {
        let c = CronExpr::parse("* * * * *").unwrap();
        let next = c.next_after(UnixMillis(0)).unwrap();
        assert_eq!(next.as_millis(), 60_000);
        assert!(c.matches(0, 0, 1, 1, 4));
    }

    #[test]
    fn cron_hourly() {
        let c = CronExpr::parse("0 * * * *").unwrap();
        assert!(c.matches(0, 5, 1, 1, 4));
        assert!(!c.matches(1, 5, 1, 1, 4));
    }

    #[test]
    fn claim_and_advance_is_idempotent_key() {
        let mut book = ScheduleBook::default();
        let agent = AgentName::parse("support-desk").unwrap();
        let instance = InstanceId::parse("nightly").unwrap();
        let cron = CronExpr::parse("0 0 * * *").unwrap();
        let due = UnixMillis(0);
        let id = ScheduleId::new();
        book.insert(Schedule {
            id: id.clone(),
            agent,
            instance,
            cron,
            payload: json!({"tick": true}),
            next_due: due,
            enabled: true,
        });
        let claimed = book.claim_and_advance(UnixMillis(1)).unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(
            claimed[0].2.idempotency_key.as_deref(),
            Some(idempotency_key(&id, due).as_str())
        );
        assert!(book.get(&id).unwrap().next_due.as_millis() > 0);
        let again = book.claim_and_advance(UnixMillis(1)).unwrap();
        assert!(again.is_empty());
    }
}
