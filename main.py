import pandas as pd
from fastapi import FastAPI
from tabula import read_pdf

from db import connect
from model import TimePeriod

app = FastAPI()
conn = connect()
cursor = conn.cursor()


def initialize_db():
    conn = connect()
    cursor = conn.cursor()
    query = """
    CREATE TABLE IF NOT EXISTS invoice (
        id INT AUTO_INCREMENT PRIMARY KEY,
        app_id VARCHAR(255),
        x_ref VARCHAR(255),
        settlement_date date,
        broker VARCHAR(255),
        sub_broker VARCHAR(255),
        borrower_name VARCHAR(255),
        description VARCHAR(255),
        total_loan_amount double,
        comm_rate double,
        upfront double,
        upfront_incl_gst double,
        CONSTRAINT invoice_constrain UNIQUE (x_ref,total_loan_amount)
    )
    """
    cursor.execute(query)
    conn.close()


def insert_data_into_db(app_id, x_ref, settlement_date, broker, sub_broker, borrower_name, description,
                        total_loan_amount, comm_rate, upfront, upfront_incl_gst):
    conn = connect()
    cursor = conn.cursor()
    query = """
    insert ignore into invoice (app_id,x_ref,settlement_date,broker,sub_broker,borrower_name,description,
    total_loan_amount,comm_rate,upfront,upfront_incl_gst) values (
    """
    query += "'" + app_id + "'," + "'" + x_ref + "'," + "STR_TO_DATE('" + settlement_date + "','%d/%m/%Y'),'" + broker + "','" + sub_broker + "','" + borrower_name + "','" + description + "'," + total_loan_amount + "," + comm_rate + "," + upfront + "," + upfront_incl_gst + ")"
    print(query)
    cursor.execute(query)
    conn.commit()
    conn.close()


@app.on_event("startup")
async def startup_event():
    initialize_db()


@app.post("/insert_data")
async def insert_data():
    df = read_pdf("Test_PDF.pdf", pages="all")  # address of pdf file
    for d in range(len(df)):
        for i in df[d].values.tolist():
            print(i)
            if pd.isna(i[1]):
                app_id = i[0].split()[0]
                x_ref = i[0].split()[1]
                settlement_date = i[2]
                broker = i[3]
                if pd.isna(i[4]):
                    sub_broker = ""
                else:
                    sub_broker = i[4]
                borrower_name = i[5].split('Upfront Commission')[0]
                description = 'Upfront Commission'
                total_loan_amount = "".join(i[7].split(','))
                comm_rate = str(i[8])
                upfront = "".join(i[9].split(','))
                upfront_incl_gst = "".join(i[10].split(','))
            else:
                app_id = i[0].split()[0]
                x_ref = i[0].split()[1]
                settlement_date = i[1]
                broker = i[2]
                sub_broker = i[3]
                borrower_name = i[3]
                description = i[4]
                total_loan_amount = "".join(i[5].split(','))
                comm_rate = str(i[6])
                upfront = "".join(i[7].split(','))
                upfront_incl_gst = "".join(i[8].split(','))
            try:
                insert_data_into_db(app_id, x_ref, settlement_date, broker, sub_broker, borrower_name, description,
                                    total_loan_amount, comm_rate, upfront, upfront_incl_gst)
            except:
                print("error while inserting into table")
    return {"status": "success"}


@app.post("/get_total_amount")
async def get_total_amount(time_period: TimePeriod):
    conn = connect()
    cursor = conn.cursor()
    query = """
        select sum(total_loan_amount) from invoice where settlement_date between
        """
    query += "STR_TO_DATE('" + time_period.start_date + "','%d/%m/%Y')" + ' and ' + "STR_TO_DATE('" + time_period.end_date + "','%d/%m/%Y')"
    print(query)
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    return {"total_amount": result}


@app.post("/get_highest_amount_by_broker")
async def get_total_amount(broker_name: str):
    conn = connect()
    cursor = conn.cursor()
    query = """
        select max(total_loan_amount) from invoice where broker = 
        """
    query += "'" + broker_name + "'"
    print(query)
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    return {"highest_amount": result}


@app.post("/get_total_amount_group_by_date")
async def get_total_amount_group_by_date():
    conn = connect()
    cursor = conn.cursor()
    query = """
            select settlement_date,sum(total_loan_amount) from invoice group by settlement_date
            """
    print(query)
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    return {"result": result}


@app.post("/get_number_of_loan_by_tier")
async def get_number_of_loan_by_tier():
    conn = connect()
    cursor = conn.cursor()
    query = """
            select settlement_date,sum(total_loan_amount) from invoice group by settlement_date
            """
    print(query)
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()
    tier_1_no = 0
    tier_2_no = 0
    tier_3_no = 0
    for r in result:
        if r[1] is not None and r[1] > 100000:
            tier_1_no += 1
        if r[1] is not None and r[1] > 50000:
            tier_2_no += 1
        if r[1] is not None and r[1] > 10000:
            tier_3_no += 1
    return {"tier_1_no": tier_1_no, "tier_2_no": tier_2_no, "tier_3_no": tier_3_no}
