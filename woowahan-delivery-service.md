# Making It Concrete: The 배민배달 Delivery Domain & Its Event Data Model

> A **faithful reconstruction** of the order–delivery domain from Woowa Brothers' tech blog post
> [*우리 팀은 카프카를 어떻게 사용하고 있을까*](https://techblog.woowahan.com/17386/) (2024-05-30, 김나은),
> with the Transactional Outbox + CDC publishing path kept as a condensed supporting section.

---

## 1. Context & scope

The **딜리버리서비스팀** (Delivery Service Team) operates **배민배달** (배달의민족's first-party delivery),
**mediating 1M+ deliveries/day**. Its defining trait — and the key to the whole data model — is that it
is a **mediator**, not the originator of orders:

> 배달의 민족에서 제공하는 여러 주문서비스(배민배달, B마트, 배민스토어)의 배민배달을 받아 여러 배달서비스
> 중 하나로 분배하고, 배달과정을 중계하고 관리하는 역할을 합니다.

It **receives** orders from several upstream order services, **distributes** each to one of several
downstream delivery services, and **relays and manages** the delivery's progress. Two server groups,
each with N instances, are in scope: **주문/배달서버** (order-delivery) persists state to MySQL and publishes events;
**분석서버** (analysis) consumes them (out of scope).

This document concentrates on the **domain and its data**: what a delivery *is*, how it relates to the
upstream order, and how its lifecycle is modeled as events. The reliable-publishing machinery
(Transactional Outbox + Debezium CDC, blog section [1]) appears only in condensed form in
[§5](#5-publishing-the-data-model-condensed).

---

## 2. Domain model

### 2.1 Actors & systems

| Actor / system | Role | Relationship to the team |
|---|---|---|
| **Upstream order services** — 배민배달 · B마트 · 배민스토어 | originate orders (주문) | the team **receives** their order events; does **not** own the order |
| **Delivery Service Team** (this system) | creates and manages a **delivery (배달)** per order; distributes and relays | **owns** the delivery |
| **Downstream delivery services (배달서비스)** | actually carry out the delivery; supply riders | one is chosen per delivery via **분배 (distribution)** |
| **Rider (라이더)** | picks up and delivers | assigned at **배차 (dispatch)** *[inf — see §3]* |
| **Operator / CS (운영자)** | adjusts distribution rules, monitors | changes 분배규칙 (distribution rules) at runtime |

### 2.2 Order vs delivery (the core distinction)

Because the team is a **mediator**, two different things — with two different identifiers, owners, and
lifecycles — must never be conflated:

| | **Order (주문)** | **Delivery (배달)** |
|---|---|---|
| Identifier | **주문식별자 `orderId`** | **배달식별자 `deliveryId`** |
| Owned by | upstream order service | **this team** |
| Lifecycle | placed / paid / cancelled (upstream) | created → dispatched → picked up → delivered |
| Where it lives | referenced in the team's data | the team's **aggregate root** (MySQL) |

The team **creates one delivery to fulfill one accepted order** *(1 order → 1 delivery; treated as 1:1
here — [inf]; the blog does not discuss split deliveries)*. This distinction drives the topic and key
plan in [§6](#6-topic--partition--key-plan).

### 2.3 Distribution & dispatch

Two domain steps decide *who* delivers, and they populate two fields on the aggregate:

- **분배 (Distribution)** — the delivery server picks **one delivery service** for the delivery, using
  in-memory **분배규칙 (distribution rules)** that operators can change at runtime. → sets
  `deliveryServiceId`. *(The propagation of rule changes across servers is the blog's Event-Bus
  subsystem — out of scope; only the outcome is in our data model.)*
- **배차 (Dispatch)** — a **rider** from the chosen delivery service is assigned, producing the
  **배차완료 (DispatchCompleted)** event. → sets `riderId`. *[inf — the blog names 배차완료 as an
  event but does not detail rider fields.]*

### 2.4 The delivery lifecycle

A delivery advances through **ordered states**, emitting an event at each step
("배달은 생성, 배차, 픽업, 완료 등 순서를 가지고 진행되며, 특정 행위마다 배달이벤트를 발행"). Two domain
subtleties drive every design decision downstream:

- **Near-simultaneous events.** 배차완료 and 픽업준비요청 can fire almost together. The producer emits
  배차완료 *then* 픽업준비요청, but a consumer could receive them in reverse order and lose track of which
  truly happened first. → **ordering must be guaranteed per delivery** (problem P1, §4).
- **Cancellation can arrive at any time.** An upstream 주문취소 produces a 배달취소; the delivery jumps to
  `CANCELLED` from whatever non-terminal state it was in. If that event is lost, a cancelled delivery
  keeps being delivered. → **events must not be lost** (problem P2, §4).

### 2.5 Domain invariants

1. **One delivery, one owner of truth.** The `Delivery` aggregate in MySQL is the source of truth for
   delivery state; events are *facts about* state changes, never the state itself.
2. **State is monotonic except for cancellation.** Forward only along `CREATED → … → DELIVERED`;
   `CANCELLED` is the sole "sideways" transition and only from a non-terminal state.
3. **Per-delivery ordering must be preserved; cross-delivery ordering is irrelevant.** Two events for
   the *same* `deliveryId` have a meaningful order; two events for *different* deliveries do not.
4. **The order is referenced, never owned.** The team stores `orderId` + which upstream service sent it,
   but the order's own lifecycle belongs upstream.

---

## 3. Data model (focused on the `Delivery` aggregate)

### 3.1 The `Delivery` aggregate

The aggregate root. Fields the blog implies or names directly are unmarked; fields reconstructed to
make the model concrete are marked **[inf]**.

```
Delivery                      -- aggregate root; one row per delivery in MySQL
  deliveryId        : string  -- 배달식별자 · PK · the event key & partition key
  orderId           : string  -- 주문식별자 · upstream reference (FK-like, not owned)
  orderServiceType  : enum    -- BAEMIN_DELIVERY | B_MART | BAEMIN_STORE   (which upstream sent it)
  deliveryServiceId : string  -- chosen via 분배 (distribution)
  riderId           : string? -- set at 배차완료                            [inf]
  status            : enum    -- CREATED | ASSIGNED | PICKUP_REQUESTED | PICKED_UP | DELIVERED | CANCELLED
  pickup            : Place   -- merchant location                          [inf, minimal]
  dropoff           : Place   -- customer location                          [inf, minimal]
  createdAt         : ts
  assignedAt        : ts?     -- stamped at 배차완료
  pickupRequestedAt : ts?     -- stamped at 픽업준비요청
  pickedUpAt        : ts?     -- stamped at 픽업
  deliveredAt       : ts?     -- stamped at 완료
  cancelledAt       : ts?     -- stamped at 배달취소
```

Each lifecycle event corresponds to **one state transition + the timestamp/field it sets** — i.e.
events are *deltas of this aggregate* (§3.4).

### 3.2 Relationships

```
   Order (upstream, referenced)                 DeliveryService (distribution target)
     orderId  ◀───────────────┐                   deliveryServiceId
     orderServiceType          │ references             ▲
                               │ (N:1, really 1:1)      │ distributed to (N:1)
                        ┌──────┴────────────────────────┴──────┐
                        │                Delivery               │
                        │  deliveryId (PK)                      │
                        │  orderId  ───────────► Order          │
                        │  deliveryServiceId ──► DeliveryService│
                        │  riderId  ───────────► Rider  [inf]   │
                        │  status, timestamps…                  │
                        └───────────────────┬───────────────────┘
                                            │ assigned at 배차 (N:1)
                                            ▼
                                        Rider  [inf]
                                          riderId
```

- **Order → Delivery: 1:1** (one delivery fulfills one accepted order) *[inf on the strict 1:1]*.
- **Delivery → DeliveryService: N:1** (many deliveries routed to one service).
- **Delivery → Rider: N:1** *[inf]* (a rider handles many deliveries over time; one at assignment).

### 3.3 Event keys: 주문식별자 vs 배달식별자

The blog is explicit that ordering is keyed by the identifier that needs it
("주문식별자, 배달식별자 등과 같이 순서관리가 필요한 식별자를 키로 관리하여 순서를 보장"). In this
subsystem, that rule follows the ownership boundary from §2.2:

- **`orderId` (주문식별자)** keys the upstream **`order`** topic — orders ordered per order.
- **`deliveryId` (배달식별자)** keys the **`delivery`** topic and partitions *all* lifecycle events —
  deliveries ordered per delivery. See [§6](#6-topic--partition--key-plan) for the topic plan.

The aggregate carries both, but its **own** identity and event key are both `deliveryId`.

### 3.4 Lifecycle events as deltas of the aggregate

Each state-changing event = a transition + the fields it stamps. Catalog (Korean terms are the blog's):

| Event | Korean | Transition | Stamps / sets |
|---|---|---|---|
| `DeliveryCreated` | 배달생성 | → `CREATED` | deliveryId, orderId, orderServiceType, pickup, dropoff, createdAt |
| `DispatchCompleted` | 배차완료 | `CREATED`/`ASSIGNED` → `ASSIGNED` | deliveryServiceId, riderId, assignedAt |
| `PickupRequested` | 픽업준비요청 | → `PICKUP_REQUESTED` | pickupRequestedAt |
| `PickedUp` | 픽업 | → `PICKED_UP` | pickedUpAt |
| `Delivered` | 완료 | → `DELIVERED` | deliveredAt |
| `DeliveryCancelled` | 배달취소 | non-terminal → `CANCELLED` | cancelledAt, reason |

> **Naming note.** 픽업준비요청 literally carries a "prep / 준비" nuance; this spec uses the shorter
> `PickupRequested` as its English handle.

**Event envelope** *(reconstructed — the blog gives no schema; serialization is Avro + Schema Registry,
[inf]):*

```
eventId       : UUID       # idempotency handle
eventType     : string     # "DispatchCompleted" | ...
aggregateType : "delivery" # routes to the `delivery` topic
aggregateId   : string     # = deliveryId → Kafka message key
occurredAt    : timestamp
version       : int        # additive-only schema evolution
payload       : <event-specific record, the deltas above>
```

**Representative payloads** (the others follow the same shape — deltas + the keys for consumer joins):

```jsonc
// DispatchCompleted (배차완료)
{
  "deliveryId": "DLV-20260628-000123",
  "orderId": "ORD-20260628-998877",
  "deliveryServiceId": "DS-ALPHA",
  "riderId": "RDR-4412",                 // [inf]
  "assignedAt": "2026-06-28T11:02:13.412+09:00"
}

// DeliveryCancelled (배달취소)
{
  "deliveryId": "DLV-20260628-000123",
  "orderId": "ORD-20260628-998877",
  "reason": "ORDER_CANCELLED",           // upstream 주문취소 → 배달취소
  "cancelledAt": "2026-06-28T11:05:51.007+09:00"
}
```

### 3.5 State machine

```
  배달생성        배차완료           픽업준비요청          픽업           완료
 ─────────▶ CREATED ─────▶ ASSIGNED ─────▶ PICKUP_REQUESTED ─────▶ PICKED_UP ─────▶ DELIVERED (terminal)
                │                                                                   ▲
                └──────────── (forward only; near-simultaneous 배차완료 ↔ 픽업준비요청) ┘

  any non-terminal state ──── 배달취소 (DeliveryCancelled) ────▶ CANCELLED (terminal)
```

Consumers validate transitions against this machine, parking/rejecting events that don't fit the
current state — the defense against the reordering described in P1.

---

## 4. The two problems (why the publishing path exists)

The data model above is only safe if its events reach consumers **in order** and **without loss**.

- **P1 · Ordering (순서보장).** Near-simultaneous 배차완료/픽업준비요청 must not be consumed in reverse order,
  or per-delivery state logic breaks. → guarantee order **per `deliveryId`**.
- **P2 · No-loss & consistency (데이터 정합성).** The MySQL state change and the Kafka event must be
  atomic; a lost 배달취소 means a cancelled delivery keeps going. → no dual-write, lossless retry.

These motivate — and are fully answered by — the condensed mechanics in §5.

---

## 5. Publishing the data model (condensed)

This section summarizes the Transactional Outbox + Debezium CDC path that carries the §3 events; see the
source post for the team's narrative.

- **Outbox bridge.** Each state change writes the business row **and** an outbox row in **one local
  MySQL transaction** (no dual-write). Outbox tables are **insert-only** and preserve stored order.

  ```sql
  CREATE TABLE delivery_outbox_1 (        -- _1.._N shards
    id BIGINT AUTO_INCREMENT PRIMARY KEY, -- monotonic = commit/binlog order
    aggregate_type VARCHAR(64),           -- 'delivery' → routed topic
    aggregate_id   VARCHAR(64),           -- = deliveryId → Kafka key
    event_type     VARCHAR(64),
    event_id       CHAR(36) UNIQUE,       -- → consumer idempotency
    payload        JSON,
    occurred_at    DATETIME(3)
  );                                       -- INSERT only; no UPDATE/DELETE
  ```

- **Sharding for throughput.** Tables are split by identifier — `delivery-outbox1/2/3` — via a stable
  `shardIndex = (hash(deliveryId) % N) + 1`, so **same `deliveryId` → same shard table** *[inf on the
  exact function]*. These are **tables, not topics**; all shards feed the single `delivery` topic.
- **CDC publish, one task per shard.** A **Debezium MySQL source connector** tails the committed
  **binlog** (log-tailing) per shard table, with **`tasks.max=1`** — the blog's explicit ordering lever
  — routing by `aggregate_type` and keying by `aggregate_id`. Publish is **at-least-once** with retry
  from the stored offset.
- **The ordering chain (P1).** `same deliveryId → same shard table → single-task connector → same
  partition (key=deliveryId) → same consumer` ⇒ 배차완료 always precedes 픽업준비요청. Break any link
  (multiple tasks / unstable shard / wrong key) and the reordering returns.
- **Consistency (P2), with a correction.** The blog says "메시지 발행에 실패하면 아웃박스테이블의
  데이터도 롤백" — **technically loose.** In a log-tailing CDC design, nothing rolls back at publish
  time: the outbox row commits *atomically with the business write first*, then Debezium publishes from
  the *already-committed* binlog with at-least-once retry. Consistency comes from **local atomicity +
  lossless retry**, not a distributed transaction. *(This is the one place fidelity means flagging the
  source's imprecision.)*

---

## 6. Topic / partition / key plan

| Topic | Key | Carries | Note |
|---|---|---|---|
| `order` | `orderId` (주문식별자) | upstream order events | referenced, not owned |
| `delivery` | **`deliveryId` (배달식별자)** | all §3.4 lifecycle events | `P` partitions, sized for consumer parallelism |
| `analysis` | `deliveryId` | re-published, preprocessed | out of scope |

N shard connectors write into `delivery`. Separately, `partition = hash(deliveryId) % P` lands all
events for one delivery on one partition. Producer sharding and broker partitioning are independent hash
functions over the **same** key, so they agree on locality without coordinating. *[inf — partition counts
not given. `N` (shard tables) and `P` (partitions) tune independent stages: producer/CDC throughput vs.
consumer parallelism. Per-delivery ordering holds for any `P ≥ 1`; size `P` for consumer parallelism and
`N` for CDC throughput.]*

---

## 7. Gaps & inferences (the honesty ledger)

The one place this reconstruction **diverges from the source's wording** rather than merely filling a
blank: the outbox rollback mechanism.

The blog writes *"메시지 발행에 실패하면 아웃박스테이블의 데이터도 롤백되기 때문에"* — implying the outbox
row rolls back when the Kafka publish fails. That **contradicts the mechanism the same post describes**:
Debezium **log-tailing** reads the **already-committed** binlog *after* the local transaction commits, so
there is no transaction spanning the publish to roll back. The accurate model is **commit-then-tail with
at-least-once retry** (§5). Confidence is **High** because the contradiction is internal — the post's own
description of binlog CDC is unambiguous; only the one summarizing sentence is loose. Faithful
reconstruction here means siding with the mechanism over the gloss, and saying so out loud rather than
silently reproducing the error.

---

## 8. References

- Source — 김나은, **우리 팀은 카프카를 어떻게 사용하고 있을까**, 우아한형제들 기술블로그, 2024-05-30.
  <https://techblog.woowahan.com/17386/>
- Pattern: **Transactional Outbox** — <https://microservices.io/patterns/data/transactional-outbox.html>
- Pattern: **Transaction log tailing** — <https://microservices.io/patterns/data/transaction-log-tailing.html>
- **Debezium** (MySQL connector, Outbox Event Router SMT) — <https://debezium.io/>
- Companion judgment doc (this repo): [`docs/superpowers/specs/2026-06-07-food-delivery-eda-design.md`](docs/superpowers/specs/2026-06-07-food-delivery-eda-design.md)
- Companion notes (this repo): [`reliability-and-at-most-once.md`](reliability-and-at-most-once.md)
