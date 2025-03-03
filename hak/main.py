import sqlite3
import pandas as pd
import matplotlib
matplotlib.use('Agg')  
import matplotlib.pyplot as plt
import seaborn as sns
from flask import Flask, request, jsonify, send_file
import os
import io

DB_NAME = "deformations.db"
CSV_FILE = "case_1.csv"
app = Flask(__name__)

def create_table():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS deformations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            time TEXT,
            sensor TEXT,
            value REAL
        )
    """)
    conn.commit()
    conn.close()

def load_data():
    df = pd.read_csv(CSV_FILE, delimiter=';', decimal=',')
    df.rename(columns={df.columns[0]: "Time"}, inplace=True)
    df["Time"] = pd.to_datetime(df["Time"], errors="coerce")
    
    records = []
    for column in df.columns[1:]:
        sensor_name = column.split(" ")[0]
        for _, row in df.iterrows():
            records.append((row["Time"].strftime("%Y-%m-%d %H:%M:%S"), sensor_name, float(str(row[column]).replace(',', '.'))))
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.executemany("INSERT INTO deformations (time, sensor, value) VALUES (?, ?, ?)", records)
    conn.commit()
    conn.close()
    print(f"Данные загружены: {len(records)} записей")

@app.route("/data", methods=["GET"])
def get_data():
    sensor = request.args.get("sensor")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    
    query = "SELECT time, sensor, value FROM deformations WHERE 1=1"
    params = []
    
    if sensor:
        query += " AND sensor = ?"
        params.append(sensor)
    if start_date:
        query += " AND time >= ?"
        params.append(start_date)
    if end_date:
        query += " AND time <= ?"
        params.append(end_date)
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    
    return jsonify([{"time": row[0], "sensor": row[1], "value": row[2]} for row in rows])

@app.route("/extremes", methods=["GET"])
def get_extremes():
    period = request.args.get("period", "day")
    sensor = request.args.get("sensor")
    if period not in ["hour", "day", "week", "month"]:
        return jsonify({"error": "Invalid period"}), 400
    
    query = f"""
        SELECT sensor,
               strftime('%Y-%m-%d %H:00:00', time) AS period_hour,
               MIN(value) AS min_value_hour,
               MAX(value) AS max_value_hour,
               strftime('%Y-%m-%d', time) AS period_day,
               MIN(value) AS min_value_day,
               MAX(value) AS max_value_day,
               strftime('%Y-%W', time) AS period_week,
               MIN(value) AS min_value_week,
               MAX(value) AS max_value_week,
               strftime('%Y-%m', time) AS period_month,
               MIN(value) AS min_value_month,
               MAX(value) AS max_value_month
        FROM deformations WHERE sensor = ?
        GROUP BY sensor, period_hour, period_day, period_week, period_month;
    """
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(query, (sensor,))
    rows = cursor.fetchall()
    conn.close()
    
    result = [{
        "sensor": row[0],
        "period": row[1 if period == "hour" else 4 if period == "day" else 7 if period == "week" else 10],
        "min_value": row[2 if period == "hour" else 5 if period == "day" else 8 if period == "week" else 11],
        "max_value": row[3 if period == "hour" else 6 if period == "day" else 9 if period == "week" else 12]
    } for row in rows]
    
    return jsonify(result)

@app.route("/graph", methods=["GET"])
def plot_graph():
    sensor = request.args.get("sensor")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    conn = sqlite3.connect(DB_NAME)
    query = "SELECT time, value FROM deformations WHERE sensor = ? AND time BETWEEN ? AND ?"
    df = pd.read_sql(query, conn, params=(sensor, start_date, end_date))
    conn.close()

    if df.empty:
        return jsonify({"error": "Нет данных для графика"}), 400

    df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M:%S", errors="coerce")

    df = df.dropna(subset=["time"])
    if df.empty:
        return jsonify({"error": "Все даты некорректны"}), 400

    plt.figure(figsize=(10, 5))
    sns.lineplot(x="time", y="value", data=df)
    plt.xticks(rotation=45)
    plt.title(f"График значений {sensor}")
    plt.xlabel("Время")
    plt.ylabel("Значение")

    img_io = io.BytesIO()
    plt.savefig(img_io, format="png")
    img_io.seek(0)
    plt.close()

    return send_file(img_io, mimetype="image/png")


if __name__ == "__main__":
    if not os.path.exists(DB_NAME):
        create_table()
        load_data()
    app.run(debug=True)
