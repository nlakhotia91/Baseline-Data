import pandas as pd
from pandas import DataFrame, Series
import numpy as np
import xlrd
data_vill = pd.read_csv("//home//isb//Documents//baseline_data_1//village_nov3_vg_wide.csv")
n=len(data_vill)

#### Get total number of househods at village level ####

data_hh=pd.DataFrame()
#data_hh["general.village"]=data_vill["general.village"]
for i in range(1, 35):
    var_name = "pop."+str(i)+"..no_hh"
    #data_hh["total_hh"] = data_vill["total_hh"]+data_vill[var_name]
    data_hh[var_name] = data_vill[var_name]
final_data=pd.DataFrame()
final_data["general.village"]=data_vill["general.village"]
x=final_data['general.village']
x=x.astype(str)
final_data['cp_id'] =x.str[:1]
final_data["total_hh"] = data_hh.sum(skipna=True, axis=1)

#### Propotion of households corresponding to official category ####35


def prop_soc(x,oc):
    data_hh_soc = pd.DataFrame()
    data_hh_soc["general.village"]=data_vill["general.village"]
    for j in range(1,35):
        data_hh_soc["pop."+str(j)+"..no_hh"]=0
        for i in range(0,n):
            if data_vill["pop."+str(j)+"..soc_cat"].ix[i]==x:
                data_hh_soc["pop."+str(j)+"..no_hh"].ix[i]=data_vill["pop."+str(j)+"..no_hh"].ix[i]
    data_hh_soc=data_hh_soc.drop(data_hh_soc.columns[0], axis=1)
    global final_data
    final_data["hh_total_"+oc]=data_hh_soc.sum(skipna=True, axis=1)
    final_data["propotion_"+oc]=final_data["hh_total_"+oc]/final_data.total_hh
    final_data=final_data.drop(final_data.columns[final_data.columns.get_loc("hh_total_"+oc)], axis=1)

prop_soc(x=1,oc="oc")
prop_soc(x=2,oc="obc")
prop_soc(x=3,oc="sc")
prop_soc(x=4,oc="st")

#### Calculate caste domination indx ####

final_data["caste_domination_idx"]=final_data['propotion_oc']**2+final_data['propotion_obc']**2+final_data["propotion_sc"]**2+final_data["propotion_st"]**2

#### Infrastructure (public & private) variable ####

final_data["road_present_y/n"]=data_vill["infra.roads.roads_yn"]
final_data["angw_present_y/n"]=data_vill["infra.angw.angw_yn"]
final_data["elec_present_y/n"]=data_vill["infra.elec.elec_yn"]
final_data["pds_present_y/n"]=data_vill["infra.pds.pds_yn"]
final_data["drnkwtr_present_y/n"]=data_vill["infra.drwat.drwat_yn"]
final_data["mblrcep_present_y/n"]=data_vill["infra.mrec.mrec_yn"]

dist_var=("infra.prim.prim_dist","infra.sec.sec_dist","infra.phc.phc_dist","infra.ptrans.ptrans_dist","infra.bank.bank_dist","infra.police.police_dist","infra.forg.forg_dist")
final_data["amenities_0-5kms_no"]=0
final_data["amenities_5-10kms_no"]=0
final_data["amenities_10onkms_no"]=0

for j in range(0,n):
    count=0
    count_5_10=0
    count_10on=0
    for i in range(0,7):
        dist_col=data_vill[dist_var[i]]
        if dist_col[j]>0 and dist_col[j]<6:
            count=count+1
            
        elif dist_col[j]>5 and dist_col[j]<11:
            count_5_10=count_5_10+1
            
        elif dist_col[j]>10:
            count_10on=count_10on+1
            
    final_data["amenities_0-5kms_no"].ix[j]=count
    final_data["amenities_5-10kms_no"].ix[j]=count_5_10
    final_data["amenities_10onkms_no"].ix[j]=count_10on

dist_var=("infra.prim.prim_yn","infra.sec.sec_yn","infra.phc.phc_yn","infra.ptrans.ptrans_yn","infra.bank.bank_yn","infra.police.police_yn","infra.forg.forg_yn")
#len(dist_var) ##7
final_data["amenities_0kms_no"]=0
for j in range(0,n):
    count_0=0
    for i in range(0,7):
        dist_col=data_vill[dist_var[i]]
        if dist_col[j]==1:
            count_0=count_0+1
    final_data["amenities_0kms_no"].ix[j]=count_0
#qc=final_data["amenities_0kms_no"]+final_data["amenities_0-5kms_no"]+final_data["amenities_5-10kms_no"]+final_data["amenities_10onkms_no"]
#final_data[:13]

#### Distributiion of land holding ####

final_data["propotion_landless"] = data_vill["lhold.lhold_lless"]/final_data["total_hh"]

#### Resources and Assets ####

final_data["comland_area"]=data_vill["resources.common"]
final_data["frstland_area"]=data_vill["resources.for_land"]
final_data["waterlvl_total"]=data_vill["resources.wlevel"]
final_data["trctors/hh"]=data_vill["resources.tractor"]/final_data["total_hh"]
final_data["bullck/hh"]=data_vill["resources.bull"]/final_data["total_hh"]
final_data["srfacewtr_no"]=data_vill["resources.irrig"]+data_vill["resources.per_str"]
final_data["grdwtr_no"]=data_vill["resources.dwell"]+data_vill["resources.bwell"]+data_vill["resources.hpump"]
#final_data.columns

#####Market Location ###

import numpy as np
final_data["mktlocatn_no"]=0
data_vill_nan=data_vill
data_vill_nan=data_vill_nan.fillna(0)
for j in range(0,n):
    count=0
    for i in range(1,17):
        data_col=data_vill_nan["mkt."+str(i)+"..mkt_loc"]
        
        if data_col[j]!=0:
            count=count+1
    #count_sub=16-count        
    final_data["mktlocatn_no"].ix[j]=count
    
#### Market Engagement variables ####

#final_data["mktlocatn_no"]=0

final_data["sold_itms_totno"]=0

def sell_item_cnt(ip_col_name,op_col_name):
    final_data[op_col_name+"_no"]=0
    for j in range(0,n):
        count=0
        for i in range(1,35):##35
            data_col=data_vill_nan["sell."+str(i)+".."+ip_col_name]#sell.1..item_id
            if data_col[j]!=0:
                count=count+1
    #count_sub=16-count        
        final_data[op_col_name+"_no"].ix[j]=count


sell_item_cnt(ip_col_name="item_id", op_col_name="sold-items")
sell_item_cnt(ip_col_name="sell_mkt", op_col_name="Sold-items_in-localmkts_no") 
sell_item_cnt(ip_col_name="sell_vill", op_col_name="Sold-items_to-villagers") #Sold-items_to-villagers_no
sell_item_cnt(ip_col_name="sell_trade", op_col_name="Sold-items_to-traders_no") 

#### Poverty and Wealth variables ####

final_data["hh_health-insur_propotion"]=data_vill["poverty.pov_hins"]/final_data["total_hh"]
final_data["hh_NREGA_propotion"]=data_vill["poverty.pov_nrega"]/final_data["total_hh"]
final_data["hh_PDS_propotion"]=data_vill["poverty.pov_pds"]/final_data["total_hh"]
final_data["hh_not-suff-food-supply_propotion"]=data_vill["poverty.pov_food"]/final_data["total_hh"]
final_data["hh_pvt-money-lnder_propotion"]=data_vill["poverty.pov_loan"]/final_data["total_hh"]
#final_data.columns

#### Gender Relationship ####

final_data["child_sex_ratio"]=data_vill["gender.m_ytot"]/data_vill["gender.f_ytot"]
final_data["f_entre_no"]=data_vill["gender.f_entre"]
final_data["gls_primary-school_propotion"]=data_vill["gender.f_pri"]/data_vill["gender.f_pri_age"]
final_data["gls_middle-school_propotion"]=data_vill["gender.f_mid"]/data_vill["gender.f_mid_age"]
final_data["brides_<15yrs_propotion"]=data_vill["gender.u15_mar"]/data_vill["gender.n_mar"]

#### Unique number of social ids within each official category in a village ####

def soc_id_oc_cnt(x,oc):
    final_data["social-ids_"+oc+"_no"]=0
    for j in range(0,n):
        count=0
        for i in range(1,35):
            col_soc_cat=data_vill_nan["pop."+str(i)+"..soc_cat"]
            if col_soc_cat[j]==x:
                col_id=data_vill_nan["pop."+str(i)+"..soc_id"]
                if col_id[j]!=0:
                    count=count+1
        #global final_data
        final_data["social-ids_"+oc+"_no"].ix[j]=count

soc_id_oc_cnt(x=1,oc="oc")
soc_id_oc_cnt(x=2,oc="obc")
soc_id_oc_cnt(x=3,oc="sc")
soc_id_oc_cnt(x=4,oc="st")

#### Livelihood strategies ####


for j in range(1,43):
    col=data_vill_nan["lstrat."+str(j)+"..lh_id"]
    data_vill_nan["lstrat."+str(j)+"..lh_id_type"]=0
    for i in range(0,n):
        if col[i] in range(100, 300):
            data_vill_nan["lstrat."+str(j)+"..lh_id_type"].ix[i]="agricultural"
        elif col[i] in range(1200,1400):
            data_vill_nan["lstrat."+str(j)+"..lh_id_type"].ix[i]="labours"
        elif col[i] in range(300,600):
            data_vill_nan["lstrat."+str(j)+"..lh_id_type"].ix[i]="livestock"
        elif col[i] in range(1000, 1200):
            data_vill_nan["lstrat."+str(j)+"..lh_id_type"].ix[i]="others"
        elif col[i] in range(600,700) or col[i] in range(900,1000):
            data_vill_nan["lstrat."+str(j)+"..lh_id_type"].ix[i]="resource_based-others"
        elif col[i] in range(701, 900):
            data_vill_nan["lstrat."+str(j)+"..lh_id_type"].ix[i]="trade/biz"
        else:
            data_vill_nan["lstrat."+str(j)+"..lh_id_type"].ix[i]="check"
#data_vill_1.loc[data_vill_1['lstrat.1..lh_id_type'] == "check"]
 
def liv_strat(liv_name):
    data_hh_soc = pd.DataFrame()
    data_hh_soc["general.village"]=data_vill_nan["general.village"]
    for j in range(1,43):
        data_hh_soc["lh"+str(j)+"..no_hh"]=0
        for i in range(0,n):
            if data_vill_nan["lstrat."+str(j)+"..lh_id_type"].ix[i]==liv_name:
                data_hh_soc["lh"+str(j)+"..no_hh"].ix[i]=data_vill_nan["lstrat."+str(j)+"..lh_prim"].ix[i] #lstrat.42..lh_prim
    data_hh_soc=data_hh_soc.drop(data_hh_soc.columns[0], axis=1)
    global final_data
    final_data["hh_total_"+liv_name]=data_hh_soc.sum(skipna=True, axis=1)
    final_data[liv_name+"-as-prim_propotion"]=final_data["hh_total_"+liv_name]/final_data.total_hh
    final_data =final_data.drop(final_data.columns[final_data.columns.get_loc("hh_total_"+liv_name)], axis=1)

liv_strat(liv_name="agricultural")
liv_strat(liv_name="others")
liv_strat(liv_name="labours")
liv_strat(liv_name="livestock")
liv_strat(liv_name="resource_based-others")
liv_strat(liv_name="trade/biz")

def liv_id_cnt(liv_name):
    final_data[liv_name+"-as-prim_income-ids_count"]=0
    for j in range(0,n):
        count=0
        for i in range(1,43):
            col_liv=data_vill_nan["lstrat."+str(i)+"..lh_id_type"]
            if col_liv[j]==liv_name:##OC
                col_id=data_vill_nan["lstrat."+str(i)+"..lh_prim"]##lstrat.1..lh_prim
                if col_id[j]!=0:
                    count=count+1
        #global final_data
        final_data[liv_name+"-as-prim_income-ids_count"].ix[j]=count

liv_id_cnt(liv_name="agricultural")
liv_id_cnt(liv_name="others")
liv_id_cnt(liv_name="labours")
liv_id_cnt(liv_name="livestock")
liv_id_cnt(liv_name="resource_based-others")
liv_id_cnt(liv_name="trade/biz")

final_data["diversification_index"] = final_data['labours-as-prim_propotion']**2+final_data['others-as-prim_propotion']**2+final_data["livestock-as-prim_propotion"]**2+final_data["agricultural-as-prim_propotion"]**2+final_data["trade/biz-as-prim_propotion"]**2+final_data["resource_based-others-as-prim_propotion"]**2

final_data.to_csv("//home//isb//Documents//baseline_data_1//new_var_vill_format_final_2.csv", index=False)

 ######################### For Quality check ###################################

'''import pandas as pd
from pandas import DataFrame, Series
import numpy as np
import xlrd
data_vill = pd.read_csv("//home//isb//Documents//baseline_data_1//village_nov3_vg_wide.csv")
n=len(data_vill)
#data_vill_nan=data_vill
#data_vill_nan=data_vill_nan.fillna(0)
for j in range(1,43):
    col=data_vill_nan["lstrat."+str(j)+"..lh_id"]
    data_vill_nan["lstrat."+str(j)+"..lh_id_type"]=0
    for i in range(0,n):
        if col[i] in range(100, 300):
            data_vill_nan["lstrat."+str(j)+"..lh_id_type"].ix[i]="agricultural"
        elif col[i] in range(1200,1400):
            data_vill_nan["lstrat."+str(j)+"..lh_id_type"].ix[i]="labours"
        elif col[i] in range(300,600):
            data_vill_nan["lstrat."+str(j)+"..lh_id_type"].ix[i]="livestock"
        elif col[i] in range(1000, 1200):
            data_vill_nan["lstrat."+str(j)+"..lh_id_type"].ix[i]="others"
        elif col[i] in range(601,700) or col[i] in range(900,1000):
            data_vill_nan["lstrat."+str(j)+"..lh_id_type"].ix[i]="resource_based-others"
        elif col[i] in range(700, 900):
            data_vill_nan["lstrat."+str(j)+"..lh_id_type"].ix[i]="trade/biz"
        else:
            data_vill_nan["lstrat."+str(j)+"..lh_id_type"].ix[i]="check"


#pd.to_csv("//home//isb//Documents//baseline_data_1//check_for-liveli.csv", index=False)

check=pd.DataFrame()
check["general.village"]=data_vill["general.village"]
def check_len(section,x,col1,col2): ## x is total number of columns, col1, col2
    
    check[section+"_duplicate_id"]=0
    check[section+"_len_tot"]=0
    check[section+"_len_remove_dup"]=0
    for j in range(0,n): ##number of rows
        l=[]
        for i in range(1,x):## number of columns
            col = data_vill_nan[col1+str(i)+col2]
            if col[j]!=0:
                l.append(col[j])
        dup_list=[]
        ids=[]
        for k in range(0, len(l)): ##
            if l[k] not in dup_list:
                dup_list.append(l[k])
            else:
                ids.append(l[k])
        ids_1=[ids]
        ids_int=str(ids_1[0])
                
        check[section+"_duplicate_id"].ix[j]=ids_int
    #list_one=[l]
    #check["id_list"].ix[j]=list_one[0]
        check[section+"_len_tot"].ix[j] = len(l)
        check[section+"_len_remove_dup"].ix[j]=len(set(l))

##lstrat.11..lh_id
check_len("livestock",43,"lstrat.","..lh_id")
##pop.1..soc_id
check_len("social_id", 35,"pop.","..soc_id")
##sell.1..item_id#
check_len("sell_items",35,"sell.","..item_id")
##mkt.1..mkt_loc
check_len("mkt_loc",17,"mkt.","..mkt_loc")

check.to_csv("//home//isb//Documents//baseline_data_1//check_ids_final.csv", index=False)'''