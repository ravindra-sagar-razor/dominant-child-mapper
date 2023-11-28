drop table if exists temp_parent_dom_child_inv_and_action_updated_tester

create table temp_parent_dom_child_inv_and_action_updated_tester as

with parent_child_map as (
    select distinct child.final_date
	,child.week_year
	,child.asin
	,nvl(parent.parent_asin,child.asin) as parent_asin
	,child.country_code
	,child.inventory_bucket
	, case 
		when child.inventory_bucket = 'Healthy Stock' then 1
		when child.inventory_bucket = 'Low Stock' then 2
		when child.inventory_bucket = 'Overstock' then 3
		else 4
	end as inventory_status_heirarchy
	,child.action_item_bucket
	, case 
		when child.action_item_bucket = 'Cash_in' then 1
		when child.action_item_bucket = 'Margin %' then 2
		else 3
	end as action_item_heirarchy
    from (select a.final_date, a.week_year, a.asin, a.country_code, a.inventory_bucket, b.action_item_bucket, dense_rank() over (order by week_year desc) as week_rank from rgbit_coupon_jeff_base_v2 as a left join temp_action_item_mixed_bucket_mapping as b on a.mixed_bucket = b.mixed_bucket) as child
    left join(
	select "child asin" as child_asin
		, "parent asin" as parent_asin
		, marketplace
	from child_parent_asin_mapping) as parent 
    on child.asin = parent.child_asin and child.country_code = parent.marketplace
    where week_rank =1
)

, dominant_inventory_status as(
	select week_year
			, parent_asin
			, country_code
			, inventory_bucket
			, inventory_status_rank
			, inventory_weighted_revenue
	from(
		select week_year
					, parent_asin
					, country_code
					, inventory_bucket
					, inventory_weighted_revenue
					, inventory_status_rank
					, lead_inventory_status
					, lag_inventory_status
					, case 
						when inventory_status_rank = 1 and inventory_bucket!= 'OOS' then 1
						when inventory_status_rank = 1 and inventory_bucket = 'OOS' and lead_inventory_status is null then 1
						when inventory_status_rank = 2 and inventory_bucket!= 'OOS' and lag_inventory_status = 'OOS' then 1
						else 0
					end as dominant_inventory_status_flag
		from(
			select *
				, rank() over (partition by week_year, country_code, parent_asin order by inventory_weighted_revenue, -1*inventory_status_heirarchy desc) as inventory_status_rank
				, lead (inventory_bucket) over (partition by week_year, country_code, parent_asin  order by inventory_weighted_revenue, -1*inventory_status_heirarchy desc) as lead_inventory_status
				, lag (inventory_bucket) over (partition by week_year, country_code, parent_asin  order by inventory_weighted_revenue, -1*inventory_status_heirarchy desc) as lag_inventory_status
			from(
				select week_year
						, parent_asin
						, country_code
						, inventory_bucket
						, inventory_status_heirarchy
						, sum(net_revenue*weight) as inventory_weighted_revenue
				from(
					select a.week_year
							, a.parent_asin
							, a.country_code
							, a.inventory_bucket
							, a.inventory_status_heirarchy
							, a.action_item_bucket
							, a.action_item_heirarchy
							, b.net_revenue
							, 1 - (DATEDIFF(month, b.final_date, a.final_date)/13) AS weight
					from parent_child_map as a
					left join tech_tables.tech_asin_country_orders_marketing_data_fbmfba_final as b
					on a.asin = b.asin and a.country_code = b.country_code
					where b.final_date >= DATEADD(month, -12, a.final_date) and DATEDIFF(day, b.final_date, a.final_date) >=0)
				group by week_year
							, parent_asin
							, country_code
							, inventory_bucket
							, inventory_status_heirarchy
			)
		)
	)
	where dominant_inventory_status_flag = 1 
)

, dominant_action_item as (
	select*
	from(
		select *
				, rank() over (partition by week_year, country_code, parent_asin order by action_item_weighted_revenue, -1*action_item_heirarchy desc) as dominant_action_rank
		from(
				select week_year
						, parent_asin
						, country_code
						, action_item_bucket
						, action_item_heirarchy
						, sum(net_revenue*weight) as action_item_weighted_revenue
				from(
					select a.week_year
							, a.parent_asin
							, a.country_code
							, a.inventory_bucket
							, a.inventory_status_heirarchy
							, a.action_item_bucket
							, a.action_item_heirarchy
							, b.net_revenue
							, 1 - (DATEDIFF(month, b.final_date, a.final_date)/13) AS weight
					from parent_child_map as a
					left join tech_tables.tech_asin_country_orders_marketing_data_fbmfba_final as b
					on a.asin = b.asin and a.country_code = b.country_code
					where b.final_date >= DATEADD(month, -12, a.final_date) and DATEDIFF(day, b.final_date, a.final_date) >=0)
				group by week_year
							, parent_asin
							, country_code
							, action_item_bucket
							, action_item_heirarchy
		)
	)
	where dominant_action_rank = 1
)

select g.*
		,h.child_TTM_net_revenue
from(
	select c.*
			, d.inventory_bucket as dominant_inventory_bucket
			, d.action_item_bucket as dominant_action_item_bucket
	from parent_child_map as c
	left join (
		select a.*
				, b.inventory_bucket
				, b.inventory_status_rank
				, b.inventory_weighted_revenue
		from dominant_action_item as a
		left join dominant_inventory_status as b
		on a.parent_asin = b.parent_asin and a.country_code = b.country_code and a.week_year = b.week_year
	) as d
	on c.parent_asin = d.parent_asin and c.country_code = d.country_code and c.week_year = d.week_year
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
order by g.week_year, g.asin, g.parent_asin, g.country_code
