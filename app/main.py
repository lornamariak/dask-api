from dask.distributed import Client
import dask.dataframe as dd
from fastapi import FastAPI

client = Client()
app = FastAPI()

# Import data
opioids_data = dd.read_csv(
    "/Users/lorna/Documents/MIDS 2022/First Semester/720 Practicing Data Science/dask/arcos_2011_2012.tsv",
    sep="\t",
    usecols=["BUYER_STATE","TRANSACTION_DATE","Combined_Labeler_Name", "MME_Conversion_Factor", "CALC_BASE_WT_IN_GM"],
    dtype={"TRANSACTION_DATE": "object"}
)
opioids_data["morphine_equivalent"] = (opioids_data["CALC_BASE_WT_IN_GM"]) * (opioids_data["MME_Conversion_Factor"])
opioids_data["date"] = dd.to_datetime(opioids_data.TRANSACTION_DATE, format = "%m%d%Y")
opioids_data["year"] = opioids_data.date.dt.year

opioids_data1 = opioids_data[["BUYER_STATE","Combined_Labeler_Name","morphine_equivalent"]]
opioids_data2 = opioids_data[["BUYER_STATE","year","Combined_Labeler_Name","morphine_equivalent"]]


#get the dataset summary 
@app.get("/")
def read_root():
    test = "testing root"
    return test
    


#BUYER States and the total they bought
@app.get("/states")
def read_state():
    total_states = opioids_data1.groupby(["BUYER_STATE"]).morphine_equivalent.sum()
    total_states_final = total_states.compute()
    return total_states_final


#enter a state and they return a total of morphine equivalent. 
@app.get("/states/{state_id}")
def read_state_purchases(state_id:str):
    total_states = opioids_data2.groupby(["BUYER_STATE","year"]).morphine_equivalent.sum().reset_index()
    state = total_states.loc[total_states["BUYER_STATE"] == state_id].compute()
    return state

#Companies and how much they sold
@app.get("/companies")
def read_companies():
    total_companies = opioids_data1.groupby(["Combined_Labeler_Name"]).morphine_equivalent.sum()
    total_companies_final = total_companies.compute()
    return total_companies_final

#companies and how much they sold 
@app.get("/companies/{company_name}")
def read_companies_sales(company_name:str):
    total_companies = opioids_data2.groupby(["Combined_Labeler_Name","year"]).morphine_equivalent.sum().reset_index()
    company = total_companies.loc[total_companies["Combined_Labeler_Name"]== company_name].compute()
    return company

#companies and how much they sold 
@app.get("/companies/states/{company_name}")
def read_companies_states(company_name:str):
    companies = opioids_data2.groupby(["year","Combined_Labeler_Name", "BUYER_STATE"]).morphine_equivalent.sum().reset_index()
    states = companies.loc[companies["Combined_Labeler_Name"]== company_name,"BUYER_STATE"].unique().compute()
    return states
