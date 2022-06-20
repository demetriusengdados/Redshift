begin;
create table if not exists {{ params.schema }}.{{ params.table }} (
  date_key date,
  total_sellers int,
  total_events int,
  total_tickets int,
  total_revenue double precision
) sortkey(date_key)
;

delete from {{ params.schema }}.{{ params.table }} where date_key = '{{ ds }}';

insert into {{ params.schema }}.{{ params.table }}
  select
    listtime::date as date_key,
    count(distinct sellerid) as total_sellers,
    count(distinct eventid) as total_events,
    sum(numtickets) as total_tickets,
    sum(totalprice) as total_revenue
  from tickit.listing
  where listtime::date = '{{ ds }}'
  group by date_key
;
end;