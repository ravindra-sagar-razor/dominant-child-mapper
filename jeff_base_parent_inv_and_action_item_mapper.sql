drop table if exists temp_parent_dom_child_inv_and_action;

create table temp_parent_dom_child_inv_and_action as
with parent_child_map as (
    select distinct child.final_date
	,child.week_year
	,child.asin
	,nvl(parent.parent_asin,child.asin) as parent_asin
	,child.country_code
	,child.inventory_bucket
	,child.action_item_bucket
    from (select *, dense_rank() over (order by week_year desc) as week_rank from rgbit_coupon_jeff_base_v2 where "inventory_bucket"!='OOS') as child
    left join(
	select "child asin" as child_asin
		, "parent asin" as parent_asin
		, marketplace
	from child_parent_asin_mapping) as parent 
    on child.asin = parent.child_asin and child.country_code = parent.marketplace
    where week_rank <= 12
)

select g.*
		,h.child_TTM_net_revenue
from(
	select distinct c.*
		,d.asin as dom_child_asin
		,d.inventory_bucket as dom_inventory_bucket
		,d.action_item_bucket as dom_action_item_bucket
	from parent_child_map as c
	left join(
	    select b.*
			, mapper.weighted_revenue
			, Rank() over (partition by b.parent_asin, b.country_code, b.week_year order by mapper.weighted_revenue, mapper.asin desc) as rank
	    from parent_child_map as b
	    left join(
	        select final_date
				, asin
				, country_code
				, sum(net_revenue*weight) as weighted_revenue
	        from(
				select base.final_date
					,base.asin
					,base.country_code
					,orders.net_revenue
					,1 - (DATEDIFF(month, orders.final_date, base.final_date)/13) AS weight
				from parent_child_map as base
				left join tech_tables.tech_asin_country_orders_marketing_data_fbmfba_final as orders
				on base.asin = orders.asin and base.country_code = orders.country_code
				where orders.final_date >= DATEADD(month, -12, base.final_date) and DATEDIFF(day, orders.final_date, base.final_date) >=0
	        )
	        group by final_date, asin, country_code
	    ) as mapper
	    on b.asin = mapper.asin and b.country_code = mapper.country_code and b.final_date = mapper.final_date
	) as d
	on c.parent_asin = d.parent_asin and c.country_code = d.country_code and c.week_year = d.week_year
	where d.rank =1	
) as g
left join (
	select e.asin
			,e.country_code
			,e.final_date
			, sum(net_revenue) as child_TTM_net_revenue
	from parent_child_map as e
	left join tech_tables.tech_asin_country_orders_marketing_data_fbmfba_final as f
	on e.asin = f.asin and e.country_code = f.country_code
	where f.final_date >= DATEADD(month, -12, e.final_date) and DATEDIFF(day, f.final_date, e.final_date) >=0
	group by e.asin
			, e.country_code
			,e.final_date
) as h
on g.asin = h.asin and g.final_date = h.final_date and g.country_code = h.country_code
order by asin, parent_asin, country_code
