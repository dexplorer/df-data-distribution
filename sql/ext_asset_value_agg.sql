select 
ta.effective_date 
, ta.asset_type 
, ta.asset_name 
, sum(cast(tac.asset_value as decimal(25, 2))) as asset_value_agg 
from dl_asset_mgmt.tasset ta 

left join dl_asset_mgmt.tacct_pos tac 
on ta.asset_id = tac.asset_id 
and tac.effective_date = ${effective_date_yyyy-mm-dd}

group by ta.effective_date, ta.asset_type, ta.asset_name 
order by ta.effective_date, ta.asset_type, ta.asset_name
;
