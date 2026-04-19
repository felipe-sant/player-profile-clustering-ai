import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DATABASE_URL = "postgresql://postgres:root@localhost:3003/example"
XLSX_PATH = "./data/players.xlsx"

def nan_to_none(value):
    """Converte NaN/NaT do pandas para None (NULL no postgres)."""
    if pd.isna(value):
        return None
    return value

def time_to_float(value):
    """
    Converte valor de tempo (datetime.time ou string HH:MM:SS) para
    fração decimal do dia, que é o formato original do Excel.
    Ex: 15:00:00 -> 0.625
    """
    if pd.isna(value):
        return None
    if hasattr(value, 'hour'):
        return (value.hour * 3600 + value.minute * 60 + value.second) / 86400
    return float(value)

def main():
    print("Lendo Players.xlsx...")
    df = pd.read_excel(XLSX_PATH)

    # O pandas interpreta essas colunas como time — converte de volta para float
    for col in ["Start Time (s)", "End Time (s)"]:
        df[col] = df[col].apply(time_to_float)

    print(f"  {len(df)} linhas carregadas.")

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # --------------------------------------------------------
        # 1. ATHLETE
        # --------------------------------------------------------
        print("\nInserindo athletes...")
        athletes = (
            df[["Athlete ID", "Athlete Position", "Athlete Groups"]]
            .drop_duplicates(subset=["Athlete ID"])
            .values.tolist()
        )

        execute_values(cur, """
            INSERT INTO athlete (id, position, grp)
            VALUES %s
            ON CONFLICT (id) DO NOTHING
        """, [
            (int(row[0]), nan_to_none(row[1]), nan_to_none(row[2]))
            for row in athletes
        ])
        print(f"  {len(athletes)} athletes processados.")

        # --------------------------------------------------------
        # 2. MATCH
        # --------------------------------------------------------
        print("\nInserindo matches...")
        matches = (
            df[["Start Date", "Start Time", "Week Start Date", "Month Start Date"]]
            .drop_duplicates(subset=["Start Date", "Start Time"])
            .values.tolist()
        )

        execute_values(cur, """
            INSERT INTO match (match_date, start_time, week_start_date, month_start_date)
            VALUES %s
            ON CONFLICT (match_date, start_time) DO NOTHING
        """, [
            (
                nan_to_none(row[0]),
                nan_to_none(row[1]),
                nan_to_none(row[2]),
                nan_to_none(row[3]),
            )
            for row in matches
        ])
        print(f"  {len(matches)} matches processados.")

        # Busca mapa (match_date, start_time) -> match.id
        cur.execute("SELECT id, match_date, start_time FROM match")
        match_map = {
            (str(row[1]), str(row[2])): row[0]
            for row in cur.fetchall()
        }

        # --------------------------------------------------------
        # 3. PERFORMANCE_SEGMENT
        # --------------------------------------------------------
        print("\nInserindo performance_segments...")

        segments = []
        skipped = 0

        for _, row in df.iterrows():
            date_key = str(row["Start Date"].date())
            time_key = str(row["Start Time"])
            match_id = match_map.get((date_key, time_key))

            if match_id is None:
                skipped += 1
                continue

            segments.append((
                int(row["Athlete ID"]),
                match_id,
                row["Segment Name"],
                nan_to_none(row["Start Time (s)"]),
                nan_to_none(row["End Time (s)"]),
                nan_to_none(row["Duration (mins)"]),
                nan_to_none(row["Session Load"]),
                nan_to_none(row["Workload"]),
                nan_to_none(row["Workload Volume"]),
                nan_to_none(row["Workload Intensity"]),
                nan_to_none(row["Distance (m)"]),
                nan_to_none(row["Metres per Minute (m)"]),
                nan_to_none(row["High Intensity Running (m)"]),
                nan_to_none(row["No. of High Intensity Events"]),
                nan_to_none(row["Sprint Distance (m)"]),
                nan_to_none(row["No. of Sprints"]),
                nan_to_none(row["Raw Top Speed (kph)"]),
                nan_to_none(row["Top Speed (kph)"]),
                nan_to_none(row["Avg Speed (kph)"]),
                nan_to_none(row["Accelerations"]),
                nan_to_none(row["Decelerations"]),
                nan_to_none(row["Percentage of Max Speed"]),
                nan_to_none(row["Percentage of Raw Max Speed KPH"]),
                nan_to_none(row["90% of Max Speed Events"]),
                nan_to_none(row["90% of Max Speed Distance (m)"]),
                nan_to_none(row["90% of Max Speed Duration (secs)"]),
                nan_to_none(row["90% of Raw Max Speed Events"]),
                nan_to_none(row["90% of Raw Max Speed Distance (m)"]),
                nan_to_none(row["90% of Raw Max Speed Duration (secs)"]),
            ))

        execute_values(cur, """
            INSERT INTO performance_segment (
                athlete_id, match_id, segment_name,
                start_time_s, end_time_s, duration_mins,
                session_load, workload, workload_volume, workload_intensity,
                distance_m, metres_per_minute,
                high_intensity_running_m, no_high_intensity_events,
                sprint_distance_m, no_sprints,
                raw_top_speed_kph, top_speed_kph, avg_speed_kph,
                accelerations, decelerations,
                pct_max_speed, pct_raw_max_speed,
                speed_90pct_events, speed_90pct_distance_m, speed_90pct_duration_secs,
                raw_speed_90pct_events, raw_speed_90pct_distance_m, raw_speed_90pct_duration_secs
            ) VALUES %s
            ON CONFLICT (athlete_id, match_id, segment_name) DO NOTHING
        """, segments)

        print(f"  {len(segments)} segmentos inseridos.")
        if skipped:
            print(f"  {skipped} linhas ignoradas (match não encontrado).")

        conn.commit()
        print("\nSeed concluído com sucesso!")

    except Exception as e:
        conn.rollback()
        print(f"\nErro durante o seed, rollback realizado: {e}")
        raise

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()