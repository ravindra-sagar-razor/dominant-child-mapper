# dominant-child-mapper
TEC-7465 - Jeff base table parent inventory status and action item bucket mapper
  -----------------------------------------------------------------------
  **Documentation for Jeff base table mapping to parent ASIN**
  -----------------------------------------------------------------------

  -----------------------------------------------------------------------

  -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  **Item**   **Description**                                                                                                                                                       **Comments**
  ---------- --------------------------------------------------------------------------------------------------------------------------------------------------------------------- ----------------------
  Current    In the Jeff base table, inventory status and action item bucket are mapped on child-ASIN x MP level and not on parent-ASIN x MP level                                 
  Scenario                                                                                                                                                                         

  Goal       Create an output table which has ASIN x MP x Week-year from the Jeff table, mapped to the corresponding parent-ASIN & dominant-child-ASIN                             The inventory status &
                                                                                                                                                                                   action item bucket for
                                                                                                                                                                                   the dominant-child act
                                                                                                                                                                                   as proxies for the
                                                                                                                                                                                   same for parent-ASIN

  Input      rgbit_coupon_jeff_base_v2                                                                                                                                             Jeff base table
  tables                                                                                                                                                                           

             child_parent_asin_mapping                                                                                                                                             Parent-Child ASIN
                                                                                                                                                                                   mapping

             tech_tables.tech_asin_country_orders_marketing_data_fbmfba_final                                                                                                      Net revenue used in
                                                                                                                                                                                   logic for identifying
                                                                                                                                                                                   dominant-child-ASIN

  Output     temp_parent_dom_child_inv_and_action                                                                                                                                  
  table                                                                                                                                                                            

  Github     [link](https://github.com/ravindra-sagar-razor/dominant-child-mapper/blob/17e6b5579250f6c80e1c77c4b2f54f406f982965/jeff_base_parent_inv_and_action_item_mapper.sql)   
  -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

**[Key terms]{.underline}**

1)  [Inventor status]{.underline}: Obtained from the Jeff base table in
    the field 'inventory_bucket'. Indicates the inventory health on ASIN
    X MP level. Can be "OOS"/"Overstock"/"Healthy Stock"/"Low Stock".

2)  [Action item bucket]{.underline}: Obtained from the Jeff base table
    in the field 'action_item_bucket'. Indicates the ASIN nature on ASIN
    X MP level. Can be "Cash_in"/"Velocity"/"Margin"/"Stable".

3)  [Dominant-child-ASIN]{.underline}: For a given parent-ASIN x MP x
    week-year combination, we identify a unique dominant-child-ASIN
    among its child-ASINs whose inventory status and action item bucket
    can act as proxy for the same of the parent-ASIN x MP. A detailed
    explanation of the logic to identify dominant-child-ASIN is given in
    the 'Notes' section.

**[Notes]{.underline}**

1)  [Input ASINs]{.underline}: Only those ASIN x MP x week-year
    combinations in the Jeff base table, that **do not have the
    inventory status set to 'OOS'** and week-year within 12 weeks of the
    latest week-year in the Jeff base table are considered for the
    mapping

2)  [Logic for identifying dominant-child-ASIN]{.underline}:

    a.  For identifying the dominant-child-ASIN, weighted TTM net
        revenue (Net revenue earned recently will have a higher
        weightage as compared to net revenue earned at an older point in
        time) is used.

    b.  The current iteration of the code uses linear weights for
        calculating weighted TTM net revenue i.e. net revenue earned
        within the week-year month has a weightage of 1, and this
        weightage decreases linearly every month, till the weightage of
        net revenue earned twelve months prior to the week-year month
        becomes zero.

    c.  The weighted TTM net revenue is then calculated using the
        formula summation of weight x net_revenue rolled up on ASIN x MP
        x week-year level

    d.  Then, for a given parent-ASIN x MP, the child-ASIN x MP x
        week-year combination having the highest weighted TTM net
        revenue is considered the dominant-child-ASIN for that
        week-year.

    e.  In case, a parent-ASIN x MP has multiple child-ASINs x MP in the
        same week-year with the same weighted TTM net revenue, the
        child-ASIN which comes last in the alpha-numeric order is taken
        as the dominant-child-ASIN

3)  [Output table]{.underline}:

    a.  Methodology: Input ASIN x MP combinations are mapped to
        parent-ASIN x MP from the parent-child mapping table. Then
        parent-ASIN x MP x week-year combinations are mapped to the
        corresponding dominant-child-ASIN x MP x week-year combination
        to obtain the final output

    b.  Only those ASIN x MP x Week-year combinations in the Jeff base
        table, that **do not have the inventory status set to 'OOS'**
        and week-year within 12 weeks of the latest week-year in the
        Jeff base table will be available in the output table as only
        these combination are considered in the input

    c.  The output will be unique on ASIN x MP x Week-year level

    d.  The dominant-child-ASIN x MP for a given parent-ASIN x MP may
        vary with the week-year as the weighted TTM net revenue for a
        child-ASIN x MP varies with the week-year

    e.  The explanation for columns in the output table is given below

  --------------------------------------------------------------------------------------------------
  **temp_parent_dom_child_inv_and_action**                              
  ------------------------------------------ -------------------------- ----------------------------
  **Field**                                  **Description**            **Source**

  final_date                                 Last day of week-year      Jeff base table

  week_year                                  Year and week no           Jeff base table
                                             combination                

  asin                                       Child-ASIN                 Jeff base table

  parent_asin                                Parent-ASIN                Parent child ASIN mapping
                                                                        table

  country_code                               Market place               Jeff base table

  inventory_bucket                           Child-ASIN inventory       Jeff base table
                                             status                     

  action_item_bucket                         Child-ASIN action item     Jeff base table
                                             bucket                     

  dom_child_asin                             Dominant-child-ASIN        Jeff base table (based on
                                                                        dominant child logic)

  dom_inventory_bucket                       Dominant-child-ASIN        Jeff base table (based on
                                             inventory status           dominant child logic)

  dom_action_item_bucket                     Dominant-child-ASIN action Jeff base table (based on
                                             item bucket                dominant child logic)
  --------------------------------------------------------------------------------------------------

**[Working code]{.underline}**

**create** **table** temp_parent_dom_child_inv_and_action **as**

**with** parent_child_map **as** (

**select** **distinct** child.final_date

,child.week_year

,child.**asin**

,**nvl**(parent.parent_asin,child.**asin**) **as** parent_asin

,child.country_code

,child.inventory_bucket

,child.action_item_bucket

**from** (**select** \*, **dense_rank**() **over** (**order** **by**
week_year **desc**) **as** week_rank **from** rgbit_coupon_jeff_base_v2
**where** \"inventory_bucket\"!=\'OOS\') **as** child

**left** **join**(

**select** \"child asin\" **as** child_asin

, \"parent asin\" **as** parent_asin

, marketplace

**from** child_parent_asin_mapping) **as** parent

**on** child.**asin** = parent.child_asin **and** child.country_code =
parent.marketplace

**where** week_rank \<= 12

)

**select** **distinct** c.\*

,d.**asin** **as** dom_child_asin

,d.inventory_bucket **as** dom_inventory_bucket

,d.action_item_bucket **as** dom_action_item_bucket

**from** parent_child_map **as** c

**left** **join**(

**select** b.\*

, mapper.weighted_revenue

, **Rank**() **over** (**partition by** b.parent_asin, b.country_code,
b.final_date **order by** mapper.weighted_revenue, mapper.**asin**
**desc**) **as** **rank**

**from** parent_child_map **as** b

**left** **join**(

**select** final_date

, **asin**

, country_code

, **sum**(net_revenue\*weight) **as** weighted_revenue

**from**(

**select** base.final_date

,base.**asin**

,base.country_code

,orders.net_revenue

,1 - (DATEDIFF(**month**, orders.final_date, base.final_date)/13) **AS**
weight

**from** parent_child_map **as** base

**left** **join**
tech_tables.tech_asin_country_orders_marketing_data_fbmfba_final **as**
orders

**on** base.**asin** = orders.**asin** **and** base.country_code =
orders.country_code

**where** orders.final_date \>= DATEADD(**month**, -12, base.final_date)
**and** DATEDIFF(**day**, orders.final_date, base.final_date) \>=0

)

**group** **by** final_date, **asin**, country_code

) **as** mapper

**on** b.**asin** = mapper.**asin** **and** b.country_code =
mapper.country_code **and** b.final_date = mapper.final_date

) **as** d

**on** c.parent_asin = d.parent_asin **and** c.country_code =
d.country_code **and** c.week_year = d.week_year

**where** d.**rank** =1

**order** **by** parent_asin, final_date, country_code

**[Code Explanation]{.underline}**

[Step 1]{.underline}: Creating CTE 'parent_child_map'**.** This gives
the ASINs x MP x week-year combinations which need mapping.

[Step 2]{.underline}: Creating the main output table:

a)  Mapping the 'parent_child_map' to
    'tech_tables.tech_asin_country_orders_marketing_data_fbmfba_final'
    to get all orders within twelve months prior to the week-year

b)  Calculating weights for each order based on order date and week-year
    difference

c)  Calculating weighted TTM net revenue on final_date x asin x
    country_code level mapper

d)  Mapping to 'parent_child_map' on asin x final_date x country_code
    level to obtain parent_asin d

e)  Identifying the dominant-child for a parent asin by ranking weighted
    TTM net revenue

f)  Mapping to 'parent_child_map' to current table based on parent_asin,
    country_code and final_date
