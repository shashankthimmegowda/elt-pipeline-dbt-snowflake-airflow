{% snapshot snap_saas_users %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='user_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}

select * from {{ ref('stg_saas_users') }}

{% endsnapshot %}
