BEGIN;

CREATE SCHEMA IF NOT EXISTS disciple_registry;
CREATE SCHEMA IF NOT EXISTS resource_allocation;

-- ============================================================
-- Bounded Context: Disciple Registry
-- ============================================================
CREATE TABLE IF NOT EXISTS disciple_registry.disciples (
    id                  uuid PRIMARY KEY,
    name                text NOT NULL,
    birthday            date,
    cultivation_realm   smallint NOT NULL
        CHECK (cultivation_realm BETWEEN 1 AND 5),
    cultivation_stage   smallint NOT NULL
        CHECK (cultivation_stage BETWEEN 1 AND 3),
    spirit_root         text NOT NULL,
    background_type     text NOT NULL DEFAULT 'UNKNOWN',
    background_note     text,
    joined_at           date NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);

-- Outbox for Disciple Registry integration events.
CREATE TABLE IF NOT EXISTS disciple_registry.outbox_events (
    event_id       uuid PRIMARY KEY,
    disciple_id    uuid NOT NULL,
    event_type     text NOT NULL,
    event_version  smallint NOT NULL DEFAULT 1
        CHECK (event_version > 0),
    previous_realm smallint NOT NULL
        CHECK (previous_realm BETWEEN 1 AND 5),
    previous_stage smallint NOT NULL
        CHECK (previous_stage BETWEEN 1 AND 3),
    current_realm  smallint NOT NULL
        CHECK (current_realm BETWEEN 1 AND 5),
    current_stage  smallint NOT NULL
        CHECK (current_stage BETWEEN 1 AND 3),
    occurred_at    timestamptz NOT NULL,
    published_at   timestamptz,
    attempts       integer NOT NULL DEFAULT 0
        CHECK (attempts >= 0),
    last_error     text,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_outbox_events_pending
    ON disciple_registry.outbox_events (created_at)
    WHERE published_at IS NULL;

-- ============================================================
-- Bounded Context: Resource Allocation
-- disciple_id is a logical reference only.
-- There is intentionally no FK across bounded contexts.
-- ============================================================
CREATE TABLE IF NOT EXISTS resource_allocation.disciple_resource_entitlements (
    disciple_id                        uuid PRIMARY KEY,
    monthly_spirit_stone_allowance     integer NOT NULL DEFAULT 0
        CHECK (monthly_spirit_stone_allowance >= 0),
    residence_level                    smallint NOT NULL DEFAULT 1
        CHECK (residence_level BETWEEN 1 AND 5),
    talisman_credits                   integer NOT NULL DEFAULT 0
        CHECK (talisman_credits >= 0),
    pill_credits                       integer NOT NULL DEFAULT 0
        CHECK (pill_credits >= 0),
    updated_at                         timestamptz NOT NULL DEFAULT now()
);

-- Consumer idempotency ledger

CREATE TABLE IF NOT EXISTS resource_allocation.processed_events (
    event_id       uuid PRIMARY KEY,
    event_type     text NOT NULL,
    disciple_id    uuid NOT NULL,
    processed_at   timestamptz NOT NULL DEFAULT now()
);

-- ============================================================
-- Seed: Disciple Registry
-- realm: 1-5, stage: 1=early, 2=middle, 3=late
-- ============================================================
INSERT INTO disciple_registry.disciples (
    id,
    name,
    birthday,
    cultivation_realm,
    cultivation_stage,
    spirit_root,
    background_type,
    background_note,
    joined_at,
    created_at,
    updated_at
) VALUES
    (
        '00000000-0000-4000-8000-000000000001',
        'Gu Chen',
        '2002-04-18',
        1,
        3,
        'FIVE_ELEMENTS',
        'LOWER_WORLD',
        'An outer disciple from the Lower Realm with no clan to support him.',
        '2026-01-15',
        '2026-01-15 08:30:00+07',
        '2026-08-20 10:15:00+07'
    ),
    (
        '00000000-0000-4000-8000-000000000002',
        'Lin An',
        '2001-09-07',
        2,
        1,
        'WOOD',
        'COMMON',
        'A disciple from an eastern village renowned for its spirit herbs.',
        '2024-03-02',
        '2024-03-02 09:00:00+07',
        '2026-07-11 14:20:00+07'
    ),
    (
        '00000000-0000-4000-8000-000000000003',
        'Yue Qing',
        '2000-12-21',
        2,
        2,
        'WATER',
        'NOBLE_CLAN',
        'A daughter of a collateral branch of the Yue Clan.',
        '2023-08-12',
        '2023-08-12 11:10:00+07',
        '2026-06-03 16:40:00+07'
    ),
    (
        '00000000-0000-4000-8000-000000000004',
        'Chen Yuan',
        '1999-06-30',
        2,
        3,
        'EARTH',
        'ELDER_SPONSORED',
        'Sponsored by an elder of the Formation Hall.',
        '2022-05-19',
        '2022-05-19 13:45:00+07',
        '2026-08-01 09:35:00+07'
    ),
    (
        '00000000-0000-4000-8000-000000000005',
        'Bai Luo',
        '1998-02-14',
        3,
        1,
        'METAL',
        'SECT_AFFILIATE',
        'Sent to cultivate by an allied sect.',
        '2021-11-01',
        '2021-11-01 07:50:00+07',
        '2026-05-22 12:00:00+07'
    ),
    (
        '00000000-0000-4000-8000-000000000006',
        'Han Zi',
        '1997-10-05',
        3,
        2,
        'FIRE',
        'COMMON',
        'A former hunter from a northern frontier town.',
        '2020-06-17',
        '2020-06-17 10:05:00+07',
        '2026-04-18 15:25:00+07'
    ),
    (
        '00000000-0000-4000-8000-000000000007',
        'Mu Xuan',
        '1996-08-26',
        3,
        3,
        'LIGHTNING',
        'ELDER_SPONSORED',
        'A core disciple under the elder of Sword Peak.',
        '2019-02-09',
        '2019-02-09 14:30:00+07',
        '2026-03-09 08:45:00+07'
    ),
    (
        '00000000-0000-4000-8000-000000000008',
        'Zhao Yao',
        '1995-05-11',
        4,
        1,
        'ICE',
        'NOBLE_CLAN',
        'An heir of the Zhao Clan from Frostfall City.',
        '2018-09-24',
        '2018-09-24 09:20:00+07',
        '2026-02-14 17:10:00+07'
    ),
    (
        '00000000-0000-4000-8000-000000000009',
        'Feng Yi',
        '1994-01-03',
        4,
        2,
        'WIND',
        'COMMON',
        'Earned merit through demon-suppression missions on the frontier.',
        '2017-04-13',
        '2017-04-13 12:00:00+07',
        '2026-01-28 11:55:00+07'
    ),
    (
        '00000000-0000-4000-8000-000000000010',
        'Xie Yun',
        '1992-11-19',
        5,
        1,
        'DARK',
        'UNKNOWN',
        'His history before entering the sect is sealed by order of the Sect Master.',
        '2015-07-07',
        '2015-07-07 06:40:00+07',
        '2026-08-25 19:30:00+07'
    )
ON CONFLICT (id) DO NOTHING;

-- ============================================================
-- Seed: Resource Allocation
-- Entitlements intentionally vary between disciples.
-- Equal cultivation realms do not guarantee equal totals.
-- ============================================================
INSERT INTO resource_allocation.disciple_resource_entitlements (
    disciple_id,
    monthly_spirit_stone_allowance,
    residence_level,
    talisman_credits,
    pill_credits,
    updated_at
) VALUES
    ('00000000-0000-4000-8000-000000000001',  30, 1,  15,  80, '2026-08-20 10:15:05+07'),
    ('00000000-0000-4000-8000-000000000002',  45, 2,  30, 150, '2026-07-11 14:20:05+07'),
    ('00000000-0000-4000-8000-000000000003',  70, 2,  55, 240, '2026-06-03 16:40:05+07'),
    ('00000000-0000-4000-8000-000000000004',  90, 3,  75, 320, '2026-08-01 09:35:05+07'),
    ('00000000-0000-4000-8000-000000000005', 110, 3,  90, 400, '2026-05-22 12:00:05+07'),
    ('00000000-0000-4000-8000-000000000006', 130, 3, 115, 480, '2026-04-18 15:25:05+07'),
    ('00000000-0000-4000-8000-000000000007', 180, 4, 170, 650, '2026-03-09 08:45:05+07'),
    ('00000000-0000-4000-8000-000000000008', 220, 4, 210, 800, '2026-02-14 17:10:05+07'),
    ('00000000-0000-4000-8000-000000000009', 260, 4, 250, 950, '2026-01-28 11:55:05+07'),
    ('00000000-0000-4000-8000-000000000010', 350, 5, 330, 1250, '2026-08-25 19:30:05+07')
ON CONFLICT (disciple_id) DO NOTHING;

COMMIT;
