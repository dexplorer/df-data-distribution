select 
ta.effective_date 
, ta.asset_type 
, ta.asset_name 
, sum(tac.asset_value) as asset_value_agg 
from dl_asset_mgmt.tassets ta 

left join dl_asset_mgmt.taccount_pos tac 
on ta.effective_date = tac.effective_date 
and ta.asset_id = tac.asset_id 

where ta.effective_date = ${effective_date_yyyy-mm-dd}

group by ta.effective_date, ta.asset_type, tac.asset_name 
order by ta.effective_date, ta.asset_type, tac.asset_name
;
