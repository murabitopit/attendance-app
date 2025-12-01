import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, time, timedelta, date, timezone
import math
import time as t
import uuid
import calendar

# --- 設定 ---
WORK_START_HOUR = 9
WORK_END_HOUR = 15
DEADLINE_APPLY = time(8, 0, 0)
MAX_DAILY_FINE = 1000

# 日本時間 (JST)
JST = timezone(timedelta(hours=9))

# --- Google Sheets 接続設定 (キャッシュ化) ---
@st.cache_resource
def connect_to_gsheets():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds_dict = dict(st.secrets["gcp_service_account"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    sheet_url = st.secrets["spreadsheet_url"]
    sh = client.open_by_url(sheet_url)
    return sh

# --- シート操作関数 ---
def init_sheets():
    try:
        sh = connect_to_gsheets()
        ws_users = sh.worksheet("users")
        if not ws_users.get_all_values():
            ws_users.append_row(["id", "name", "rest_balance", "paid_leave_balance", "initial_fine", "last_reset_week", "last_reset_month"])
        ws_records = sh.worksheet("records")
        if not ws_records.get_all_values():
            ws_records.append_row(["id", "user_id", "date", "clock_in", "clock_out", "status", "fine", "note"])
    except Exception as e:
        st.error(f"シート接続エラー: {e}")

@st.cache_data(ttl=5)
def get_users_stable():
    if 'cached_users_df' not in st.session_state:
        st.session_state.cached_users_df = pd.DataFrame()
    
    for _ in range(3):
        try:
            sh = connect_to_gsheets()
            ws = sh.worksheet("users")
            data = ws.get_all_records()
            df = pd.DataFrame(data)
            expected_cols = ["id", "name", "rest_balance", "paid_leave_balance", "initial_fine", "last_reset_week", "last_reset_month"]
            if df.empty or not set(expected_cols).issubset(df.columns):
                return pd.DataFrame(columns=expected_cols)
            st.session_state.cached_users_df = df
            return df
        except Exception:
            t.sleep(1)
    return st.session_state.cached_users_df

@st.cache_data(ttl=5)
def get_records_stable():
    if 'cached_records_df' not in st.session_state:
        st.session_state.cached_records_df = pd.DataFrame()
        
    for _ in range(3):
        try:
            sh = connect_to_gsheets()
            ws = sh.worksheet("records")
            data = ws.get_all_records()
            df = pd.DataFrame(data)
            expected_cols = ["id", "user_id", "date", "clock_in", "clock_out", "status", "fine", "note"]
            if df.empty or not set(expected_cols).issubset(df.columns):
                return pd.DataFrame(columns=expected_cols)
            st.session_state.cached_records_df = df
            return df
        except Exception:
            t.sleep(1)
    return st.session_state.cached_records_df

def clear_cache():
    get_users_stable.clear()
    get_records_stable.clear()

def find_row_num(worksheet, col_name, value):
    try:
        cell = worksheet.find(str(value), in_column=worksheet.find(col_name).col)
        return cell.row if cell else None
    except:
        return None

def add_user(name):
    sh = connect_to_gsheets()
    ws = sh.worksheet("users")
    new_id = str(uuid.uuid4())
    ws.append_row([new_id, name, 0, 0, 0, "", ""])
    clear_cache()

def update_user_balance(user_id, col_name, amount):
    sh = connect_to_gsheets()
    ws = sh.worksheet("users")
    row = find_row_num(ws, "id", user_id)
    if row:
        col = ws.find(col_name).col
        val = ws.cell(row, col).value
        current_val = int(val) if val else 0
        ws.update_cell(row, col, current_val + amount)
        clear_cache()

def update_user_field_direct(user_id, col_name, value):
    sh = connect_to_gsheets()
    ws = sh.worksheet("users")
    row = find_row_num(ws, "id", user_id)
    if row:
        col = ws.find(col_name).col
        ws.update_cell(row, col, value)

def delete_user_data(user_id):
    sh = connect_to_gsheets()
    ws_u = sh.worksheet("users")
    row = find_row_num(ws_u, "id", user_id)
    if row: ws_u.delete_rows(row)
    clear_cache()

# --- 重複チェック関数 ---
def has_record_for_date(user_id, date_str):
    df = get_records_stable()
    if df.empty: return False
    exists = df[(df['user_id'].astype(str) == str(user_id)) & (df['date'] == date_str)]
    return not exists.empty

# --- レコード追加 (重複チェック付き) ---
def add_record(user_id, status, fine=0, note="", clock_in="", clock_out="", date_str=None):
    sh = connect_to_gsheets()
    ws = sh.worksheet("records")
    if date_str is None:
        now = datetime.now(JST)
        date_str = now.strftime('%Y-%m-%d')
    
    if has_record_for_date(user_id, date_str):
        return False, "本日は既に記録が存在します"

    rec_id = str(uuid.uuid4())
    ws.append_row([rec_id, user_id, date_str, clock_in, clock_out, status, fine, note])
    clear_cache()
    return True, "登録しました"

def update_record_out(user_id, clock_out_obj, status, fine, note_append):
    sh = connect_to_gsheets()
    ws = sh.worksheet("records")
    if isinstance(clock_out_obj, datetime):
        clock_out_str = clock_out_obj.strftime('%H:%M:%S')
    else:
        clock_out_str = str(clock_out_obj)

    records = ws.get_all_records()
    target_row_idx = -1
    record_data = None
    
    for i, r in enumerate(reversed(records)):
        if str(r['user_id']) == str(user_id) and (r['clock_out'] is None or str(r['clock_out']).strip() == ""):
            real_index = (len(records) - 1) - i
            target_row_idx = real_index + 2
            record_data = r
            break
            
    if target_row_idx > 0 and record_data:
        try:
            clock_in_date = datetime.strptime(record_data['date'], '%Y-%m-%d').date()
        except:
            clock_in_date = datetime.now(JST).date()

        today_date = datetime.now(JST).date()
        early_fine = 0
        if today_date > clock_in_date:
            early_fine = 0 
        else:
            is_holiday_work = "休日出勤" in str(record_data['status']) or "土日祝" in str(record_data['note'])
            if not is_holiday_work:
                if isinstance(clock_out_obj, datetime):
                    early_fine = calculate_early_fine(clock_out_obj)
        
        current_status = record_data['status']
        status_add = "/早退" if early_fine > 0 else ""
        new_status = current_status + status_add if "退勤済" not in current_status else current_status

        current_fine = int(record_data['fine']) if record_data['fine'] else 0
        total_fine = current_fine + early_fine
        if total_fine > MAX_DAILY_FINE: total_fine = MAX_DAILY_FINE

        current_note = ws.cell(target_row_idx, 8).value or ""
        new_note = (str(current_note) + " " + note_append).strip()
        
        ws.update_cell(target_row_idx, 5, clock_out_str)
        ws.update_cell(target_row_idx, 6, new_status)
        ws.update_cell(target_row_idx, 7, total_fine)
        ws.update_cell(target_row_idx, 8, new_note)
        clear_cache()
        return True
    return False

def admin_update_record_direct(rec_id, clock_in, clock_out, status, fine, note):
    sh = connect_to_gsheets()
    ws = sh.worksheet("records")
    row = find_row_num(ws, "id", rec_id)
    if row:
        ws.update_cell(row, 4, clock_in)
        ws.update_cell(row, 5, clock_out)
        ws.update_cell(row, 6, status)
        ws.update_cell(row, 7, fine)
        ws.update_cell(row, 8, note)
        clear_cache()

def update_initial_fine(user_id, amount):
    sh = connect_to_gsheets()
    ws = sh.worksheet("users")
    row = find_row_num(ws, "id", user_id)
    if row:
        col = ws.find("initial_fine").col
        ws.update_cell(row, col, amount)
        clear_cache()

def update_user_name(user_id, new_name):
    sh = connect_to_gsheets()
    ws = sh.worksheet("users")
    current_users = get_users_stable()
    if not current_users.empty:
        exists = current_users[(current_users['name'] == new_name) & (current_users['id'].astype(str) != str(user_id))]
        if not exists.empty:
            return False, "その名前は既に使用されています"
    row = find_row_num(ws, "id", user_id)
    if row:
        col = ws.find("name").col
        ws.update_cell(row, col, new_name)
        clear_cache()
        return True, "名前を変更しました"
    return False, "ユーザーが見つかりません"

def apply_leave(user_id, leave_type, target_date):
    date_str = target_date.strftime('%Y-%m-%d')
    if has_record_for_date(user_id, date_str):
        return False, f"{date_str} は既に記録があります"

    today = datetime.now(JST).date()
    now_time = datetime.now(JST).time()
    
    if leave_type == "有休":
        if target_date == today and now_time > DEADLINE_APPLY:
            return False, "当日の有給申請は8:00までです"
        if target_date < today:
            return False, "過去の日付での申請はできません"

    sh = connect_to_gsheets()
    ws = sh.worksheet("records")
    rec_id = str(uuid.uuid4())
    ws.append_row([rec_id, user_id, date_str, "-", "-", leave_type, 0, "申請利用"])
    clear_cache()
    return True, f"{date_str} の「{leave_type}」を登録しました"

def register_absence(user_id):
    success, msg = add_record(user_id, "欠勤", MAX_DAILY_FINE, "手動欠勤登録")
    if success: st.toast(f"欠勤を登録しました。(罰金{MAX_DAILY_FINE}円)")
    else: st.error(msg)

# --- ロジック ---
def is_weekend(dt):
    return dt.weekday() >= 5

def calculate_late_fine(check_in_dt):
    hour = check_in_dt.hour
    if hour < WORK_START_HOUR: return 0, "通常"
    if hour == 9: return 500, "遅刻"
    elif hour == 10: return 600, "遅刻"
    elif hour == 11: return 700, "遅刻"
    elif hour == 12: return 800, "遅刻"
    elif hour == 13: return 900, "遅刻"
    else: return 1000, "欠勤(遅刻超過)"

def calculate_early_fine(check_out_dt):
    end_dt = check_out_dt.replace(hour=WORK_END_HOUR, minute=0, second=0, microsecond=0)
    if check_out_dt >= end_dt: return 0
    diff = end_dt - check_out_dt
    hours_early = math.ceil(diff.total_seconds() / 3600)
    return hours_early * 100

def get_week_label(date_str):
    try:
        dt = pd.to_datetime(date_str)
        week_num = (dt.day - 1) // 7 + 1
        return f"{dt.month}.{week_num}"
    except:
        return ""

# --- 自動処理 ---
def auto_fill_missing_days(user_id, current_rest_balance):
    sh = connect_to_gsheets()
    ws_r = sh.worksheet("records")
    ws_u = sh.worksheet("users")
    all_recs = ws_r.get_all_records()
    user_recs = [r for r in all_recs if str(r['user_id']) == str(user_id)]
    existing_dates = set([r['date'] for r in user_recs])
    today = datetime.now(JST).date()
    start_date = date(today.year, today.month, 1)
    temp_rest_balance = current_rest_balance
    fill_log = []
    check_date = start_date
    while check_date < today:
        date_s = check_date.strftime('%Y-%m-%d')
        if not is_weekend(check_date) and date_s not in existing_dates:
            rec_id = str(uuid.uuid4())
            if temp_rest_balance > 0:
                ws_r.append_row([rec_id, user_id, date_s, "", "", "休み", 0, "自動適用"])
                temp_rest_balance -= 1
                fill_log.append(f"{date_s}: 休み(残消化)")
            else:
                ws_r.append_row([rec_id, user_id, date_s, "", "", "欠勤", 1000, "自動適用"])
                fill_log.append(f"{date_s}: 欠勤(¥1000)")
        check_date += timedelta(days=1)
    
    if temp_rest_balance != current_rest_balance:
        row = find_row_num(ws_u, "id", user_id)
        col = ws_u.find("rest_balance").col
        ws_u.update_cell(row, col, temp_rest_balance)
    if fill_log:
        clear_cache()
        return fill_log
    return []

def auto_force_checkout():
    if 'last_force_checkout' in st.session_state:
        if (datetime.now(JST) - st.session_state.last_force_checkout).total_seconds() < 60:
            return
    try:
        sh = connect_to_gsheets()
        ws = sh.worksheet("records")
        records = ws.get_all_records()
        now_dt = datetime.now(JST)
        today_str = now_dt.strftime('%Y-%m-%d')
        force_time_str = "23:55:00"
        updated_count = 0
        for i, r in enumerate(records):
            if r['clock_out'] is None or str(r['clock_out']).strip() == "":
                rec_date_str = r['date']
                should_close = False
                if rec_date_str < today_str: should_close = True
                elif rec_date_str == today_str:
                    if now_dt.hour == 23 and now_dt.minute >= 55: should_close = True
                if should_close:
                    row_idx = i + 2
                    current_note = r['note'] or ""
                    new_note = (str(current_note) + " (強制退勤)").strip()
                    ws.update_cell(row_idx, 5, force_time_str)
                    ws.update_cell(row_idx, 8, new_note)
                    updated_count += 1
        if updated_count > 0:
            clear_cache()
            st.toast(f"{updated_count}件の未退勤レコードを23:55で締めました")
        st.session_state.last_force_checkout = now_dt
    except Exception: pass

def run_global_auto_grant():
    if 'last_check' in st.session_state:
        if (datetime.now(JST) - st.session_state.last_check).total_seconds() < 60: return
    try:
        users_df = get_users_stable()
        today = datetime.now(JST)
        cur_week = today.strftime("%Y-%W")
        cur_month = today.strftime("%Y-%m")
        updates = False
        for index, u in users_df.iterrows():
            uid = str(u['id'])
            last_w = str(u['last_reset_week'])
            last_m = str(u['last_reset_month'])
            if today.weekday() == 0 and last_w != cur_week:
                update_user_field_direct(uid, "rest_balance", 1)
                update_user_field_direct(uid, "last_reset_week", cur_week)
                st.toast(f"月曜日: {u['name']}さんの休みリセット")
                updates = True
            if today.day == 1 and last_m != cur_month:
                update_user_field_direct(uid, "paid_leave_balance", 2)
                update_user_field_direct(uid, "last_reset_month", cur_month)
                st.toast(f"月初: {u['name']}さんの有給リセット")
                updates = True
        if updates: clear_cache()
        st.session_state.last_check = datetime.now(JST)
    except Exception: pass

def admin_force_grant_all(grant_type):
    sh = connect_to_gsheets()
    ws = sh.worksheet("users")
    users = ws.get_all_records()
    today = datetime.now(JST)
    cur_week = today.strftime("%Y-%W")
    cur_month = today.strftime("%Y-%m")
    count = 0
    for i, u in enumerate(users):
        row = i + 2 
        if grant_type == "rest":
            col_bal = ws.find("rest_balance").col
            col_last = ws.find("last_reset_week").col
            ws.update_cell(row, col_bal, 1)
            ws.update_cell(row, col_last, cur_week)
            count += 1
        elif grant_type == "paid":
            col_bal = ws.find("paid_leave_balance").col
            col_last = ws.find("last_reset_month").col
            ws.update_cell(row, col_bal, 2)
            ws.update_cell(row, col_last, cur_month)
            count += 1
    clear_cache()
    return f"{count}名のデータをリセットしました。"

def admin_update_record(record_id, edit_date, new_in_t, new_out_t, new_note, mode_override):
    msg_type = "success"
    msg = ""
    if mode_override == "自動計算 (時刻から判定)":
        dt_in = datetime.combine(edit_date, new_in_t)
        dt_out = datetime.combine(edit_date, new_out_t)
        late_fine, status = calculate_late_fine(dt_in)
        early_fine = calculate_early_fine(dt_out)
        total_fine = late_fine + early_fine
        if total_fine > MAX_DAILY_FINE: total_fine = MAX_DAILY_FINE
        if early_fine > 0: status += "/早退"
        if late_fine == 1000: status = "欠勤(遅刻超過)"
        admin_update_record_direct(record_id, new_in_t.strftime('%H:%M:%S'), new_out_t.strftime('%H:%M:%S'), status, total_fine, new_note)
        msg = f"再計算完了: {status}"
    elif mode_override == "「休み」に変更":
        admin_update_record_direct(record_id, "", "", "休み", 0, new_note + " (管理者変更)")
        msg = "ステータスを「休み」に変更しました。(残数手動修正要)"
        msg_type = "warning"
    elif mode_override == "「有休」に変更":
        admin_update_record_direct(record_id, "", "", "有休", 0, new_note + " (管理者変更)")
        msg = "ステータスを「有休」に変更しました。(残数手動修正要)"
        msg_type = "warning"
    return msg, msg_type

def generate_calendar_html(year, month, df_data, user_name):
    cal = calendar.Calendar(firstweekday=6) 
    month_days = cal.monthdayscalendar(year, month)
    html = f"""
    <style>
        .calendar-container {{ width: 100%; overflow-x: auto; }}
        .calendar-table {{ width: 100%; min_width: 600px; border-collapse: collapse; table-layout: fixed; }}
        .calendar-table th {{ background-color: #f0f2f6; color: #31333F; border: 1px solid #e0e0e0; padding: 8px; text-align: center; font-weight: bold; }}
        .calendar-table td {{ border: 1px solid #e0e0e0; vertical-align: top; padding: 5px; height: 80px; background-color: #ffffff; }}
        .date-num {{ font-weight: bold; margin-bottom: 5px; color: #555; }}
        .event-box {{ font-size: 0.85em; padding: 2px 4px; margin-bottom: 2px; border-radius: 4px; background-color: #f8f9fa; border-left: 3px solid #ccc; }}
        .event-fine {{ background-color: #ffebee; border-left: 3px solid #ff4b4b; color: #a00; }}
        .event-ok {{ border-left: 3px solid #00c853; color: #007029; }}
        .event-rest {{ border-left: 3px solid #2962ff; color: #0039cb; }}
        .empty-day {{ background-color: #f9f9f9; }}
    </style>
    <div class="calendar-container">
        <table class="calendar-table">
            <thead>
                <tr><th style="color:red;">日</th><th>月</th><th>火</th><th>水</th><th>木</th><th>金</th><th style="color:blue;">土</th></tr>
            </thead>
            <tbody>
    """
    for week in month_days:
        if sum(week) == 0: continue
        html += "<tr>"
        for day in week:
            if day == 0: html += "<td class='empty-day'></td>"
            else:
                day_rec = df_data[df_data['date_dt'].dt.day == day]
                cell_content = f"<div class='date-num'>{day}</div>"
                if not day_rec.empty:
                    for _, r in day_rec.iterrows():
                        fine = int(r['fine'])
                        status = r['status']
                        if fine > 0:
                            css_class = "event-fine"
                            text = f"¥{fine:,}<br>{status}"
                        elif "休み" in status:
                            css_class = "event-rest"
                            text = status
                        else:
                            css_class = "event-ok"
                            text = status
                        cell_content += f"<div class='event-box {css_class}'>{text}</div>"
                html += f"<td>{cell_content}</td>"
        html += "</tr>"
    html += "</tbody></table></div>"
    return html

# --- メインアプリ ---
def main():
    st.set_page_config(page_title="M1出勤管理", layout="wide")
    st.title(f"M1 出勤管理")
    
    if 'init_done' not in st.session_state:
        init_sheets()
        st.session_state.init_done = True
    
    run_global_auto_grant()
    auto_force_checkout()

    users = get_users_stable()
    
    if users is None or users.empty:
        user_names = {}
    else:
        user_names = {row['name']: str(row['id']) for index, row in users.iterrows()}
    
    if 'delete_confirm_id' not in st.session_state: st.session_state.delete_confirm_id = None
    if 'last_checked_user' not in st.session_state: st.session_state.last_checked_user = None

    st.write("##### 👤 使用者を選択してください")
    selected_user_name = st.selectbox("名前を選択", ["(選択してください)"] + list(user_names.keys()), label_visibility="collapsed", key="main_user_selector")
    
    if selected_user_name != "(選択してください)":
        user_id = user_names[selected_user_name]
        
        if st.session_state.last_checked_user != user_id:
            u_current = users[users['id'].astype(str) == user_id].iloc[0]
            filled_logs = auto_fill_missing_days(user_id, int(u_current['rest_balance']))
            st.session_state.last_checked_user = user_id 
            if filled_logs:
                for log in filled_logs:
                    st.toast(f"自動登録: {log}")
                t.sleep(2)
                st.rerun()

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["打刻・申請", "罰金集計", "休暇管理", "全ログ", "名簿登録", "管理者"])

    # --- Tab 1: 打刻 ---
    with tab1:
        if selected_user_name != "(選択してください)":
            user_id = user_names[selected_user_name]
            u_row = users[users['id'].astype(str) == user_id].iloc[0]
            
            st.write(f"### {selected_user_name} さんの操作")
            col1, col2 = st.columns([1, 1])
            with col1:
                st.info(f"現在: {datetime.now(JST).strftime('%m/%d %H:%M')}")
                is_holiday = is_weekend(datetime.now(JST))
                holiday_chk = st.checkbox("祝日・休日出勤 (罰金なし)", value=is_holiday)
                
                if st.button("出勤 🟢", type="primary", use_container_width=True):
                    now = datetime.now(JST)
                    fine, status = 0, "休日出勤"
                    if not (is_holiday or holiday_chk): fine, status = calculate_late_fine(now)
                    if fine > MAX_DAILY_FINE: fine = MAX_DAILY_FINE
                    
                    success, msg = add_record(user_id, status, fine, clock_in=now.strftime('%H:%M:%S'), note="土日祝" if (is_holiday or holiday_chk) else "")
                    
                    if success:
                        st.toast(f"出勤しました ({status})"); st.success("出勤しました"); t.sleep(2); st.rerun()
                    else:
                        st.error(msg)

                with st.form(key="clock_out_form", clear_on_submit=True):
                    note = st.text_input("退勤備考")
                    if st.form_submit_button("退勤 🔴", use_container_width=True):
                        now = datetime.now(JST)
                        early_fine = 0
                        if not (is_holiday or holiday_chk): early_fine = calculate_early_fine(now)
                        status_add = "/早退" if early_fine > 0 else ""
                        if update_record_out(user_id, now, "退勤済"+status_add, early_fine, note):
                            st.toast("退勤しました"); st.success("退勤しました"); t.sleep(3); st.rerun()
                        else: st.error("出勤記録が見つかりません")
            with col2:
                st.markdown(f"""
                <div style="background-color:#f0f2f6; padding:10px; border-radius:5px; margin-bottom:10px;">
                    <strong>現在の残数:</strong> 休 <span style="font-size:1.2em; color:blue;">{u_row['rest_balance']}</span> / 有 <span style="font-size:1.2em; color:green;">{u_row['paid_leave_balance']}</span>
                </div>""", unsafe_allow_html=True)
                with st.form(key="leave_form", clear_on_submit=True):
                    t_date = st.date_input("有給日付", value=datetime.now(JST))
                    c1, c2 = st.columns(2)
                    if c1.form_submit_button("休み使用 (本日)"):
                        if u_row['rest_balance'] > 0:
                            update_user_balance(user_id, "rest_balance", -1)
                            success, msg = add_record(user_id, "休み", 0, "申請利用", date_str=datetime.now(JST).strftime('%Y-%m-%d'))
                            if success: st.toast("休みを使用しました"); st.success("休みを使用しました"); t.sleep(3); st.rerun()
                            else: st.error(msg)
                        else: st.error("残数がありません")
                    if c2.form_submit_button("有給申請"):
                        if u_row['paid_leave_balance'] > 0:
                            success, msg = apply_leave(user_id, "有休", t_date)
                            if success:
                                update_user_balance(user_id, "paid_leave_balance", -1)
                                st.toast("有給を申請しました"); st.success("有給を申請しました"); t.sleep(3); st.rerun()
                            else: st.error(msg)
                        else: st.error("残数がありません")
                st.divider()
                if st.button("無断・通常欠勤 (¥1000)", use_container_width=True):
                    register_absence(user_id); t.sleep(3); st.rerun()
                with st.expander("特別欠勤 (¥0)"):
                    with st.form(key="sp_abs_form", clear_on_submit=True):
                        reas = st.selectbox("理由", ["風邪(特殊)", "就活", "学校関連", "その他"])
                        detail = st.text_input("詳細")
                        if st.form_submit_button("確定", type="secondary"):
                            final_reason = reas if reas != "その他" else detail
                            success, msg = add_record(user_id, "特別欠勤", 0, final_reason)
                            if success: st.toast("登録しました"); st.success("登録しました"); t.sleep(3); st.rerun()
                            else: st.error(msg)
        else: st.info("👆 上のボックスから名前を選択してください")

    # --- Tab 2: 罰金 ---
    with tab2:
        st.subheader("🗓️ 罰金カレンダー")
        now_t = datetime.now(JST)
        c_y, c_m, c_u = st.columns([1, 1, 2])
        sel_year = c_y.number_input("年", value=now_t.year, step=1)
        sel_month = c_m.number_input("月", value=now_t.month, min_value=1, max_value=12, step=1)
        
        def_index = list(user_names.keys()).index(selected_user_name) if selected_user_name in user_names else 0
        cal_user = c_u.selectbox("表示する人", list(user_names.keys()), index=def_index)
        cal_uid = user_names[cal_user]
        
        df = get_records_stable()
        if not df.empty and not users.empty:
            df['date_dt'] = pd.to_datetime(df['date'])
            df_m = df[(df['date_dt'].dt.year == sel_year) & 
                      (df['date_dt'].dt.month == sel_month) & 
                      (df['user_id'].astype(str) == cal_uid)].copy()
            df_m['fine'] = pd.to_numeric(df_m['fine'], errors='coerce').fillna(0)
            
            cal_html = generate_calendar_html(sel_year, sel_month, df_m, cal_user)
            st.markdown(cal_html, unsafe_allow_html=True)
            
            total_fine = df_m['fine'].sum()
            st.info(f"💰 {cal_user} さんの {sel_month}月 罰金合計: ¥{int(total_fine):,}")
            
            st.divider()
            st.subheader("📊 週別・累計リスト (全員)")
            
            df_all_m = df[(df['date_dt'].dt.year == sel_year) & (df['date_dt'].dt.month == sel_month)].copy()
            df_all_m['fine'] = pd.to_numeric(df_all_m['fine'], errors='coerce').fillna(0)
            
            users['id'] = users['id'].astype(str)
            if not df_all_m.empty:
                df_all_m['user_id'] = df_all_m['user_id'].astype(str)
                merged = pd.merge(df_all_m, users[['id', 'name']], left_on='user_id', right_on='id', how='left')
                merged['week'] = merged['date'].apply(get_week_label)
                pivot = merged.pivot_table(index='name', columns='week', values='fine', aggfunc='sum', fill_value=0)
            else:
                pivot = pd.DataFrame()

            u_init = users[['name', 'initial_fine']].set_index('name')
            u_init['initial_fine'] = pd.to_numeric(u_init['initial_fine'], errors='coerce').fillna(0)
            pivot = pivot.join(u_init, how='outer').fillna(0)
            pivot.rename(columns={'initial_fine': '運用前罰金'}, inplace=True)
            pivot['Total'] = pivot.sum(axis=1)
            cols = ['運用前罰金'] + [c for c in pivot.columns if c not in ['運用前罰金', 'Total']] + ['Total']
            st.dataframe(pivot[cols], use_container_width=True)
        else: st.info("データがありません")

    # --- Tab 3: 休暇管理 ---
    with tab3:
        st.write("#### 🔹 休暇可能な残数")
        if not users.empty:
            view_df = users[['name', 'rest_balance', 'paid_leave_balance']].copy()
            view_df.columns = ['名前', '休み(残)', '有休(残)']
            df_r = get_records_stable()
            usage_data = []
            if not df_r.empty:
                df_r['user_id'] = df_r['user_id'].astype(str)
                for idx, u_row in users.iterrows():
                    uid = str(u_row['id'])
                    u_recs = df_r[df_r['user_id'] == uid]
                    rest_used = len(u_recs[u_recs['status'] == '休み'])
                    paid_used = len(u_recs[u_recs['status'] == '有休'])
                    usage_data.append({'名前': u_row['name'], '休み(使用)': rest_used, '有休(使用)': paid_used})
            df_usage = pd.DataFrame(usage_data)
            if df_usage.empty: df_usage = pd.DataFrame(columns=['名前', '休み(使用)', '有休(使用)'])
            c3_1, c3_2 = st.columns(2)
            with c3_1: st.dataframe(view_df.style.applymap(lambda x: 'color:blue', subset=['休み(残)']).applymap(lambda x: 'color:green', subset=['有休(残)']), use_container_width=True)
            with c3_2: st.dataframe(df_usage, use_container_width=True)

    # --- Tab 4: 全ログ ---
    with tab4:
        df = get_records_stable()
        if not df.empty:
            users['id'] = users['id'].astype(str)
            df['user_id'] = df['user_id'].astype(str)
            merged = pd.merge(df, users[['id', 'name']], left_on='user_id', right_on='id', how='left')
            merged['fine'] = pd.to_numeric(merged['fine'], errors='coerce').fillna(0).astype(int)
            st.dataframe(merged[['date', 'name', 'clock_in', 'clock_out', 'status', 'fine', 'note']].iloc[::-1], use_container_width=True)

    # --- Tab 5: 名簿 ---
    with tab5:
        with st.form("reg_user", clear_on_submit=True):
            nn = st.text_input("氏名")
            if st.form_submit_button("登録"):
                add_user(nn)
                st.toast("登録しました"); st.success("登録しました"); t.sleep(2); st.rerun()
        st.write("---")
        if not users.empty:
            for i, row in users.iterrows():
                with st.expander(f"👤 {row['name']}"):
                    with st.form(key=f"edit_user_{row['id']}"):
                        new_name_input = st.text_input("名前の修正", value=row['name'])
                        if st.form_submit_button("更新"):
                            if new_name_input != row['name']:
                                success, msg_u = update_user_name(str(row['id']), new_name_input)
                                if success: st.toast(msg_u); st.success(msg_u); t.sleep(3); st.rerun()
                                else: st.error(msg_u)
                            else: st.info("変更なし")
                    if st.button("削除 (注意)", key=f"del_{row['id']}"):
                        if 'delete_confirm_id' in st.session_state and st.session_state.delete_confirm_id == row['id']:
                            delete_user_data(str(row['id']))
                            st.session_state.delete_confirm_id = None
                            st.toast("削除しました"); st.success("削除しました"); t.sleep(2); st.rerun()
                        else:
                            st.session_state.delete_confirm_id = row['id']
                            st.warning("もう一度押すと削除されます")

    # --- Tab 6: 管理者 ---
    with tab6:
        st.write("### 🛠 管理者メニュー")
        with st.expander("🚨 緊急用: 全員への休暇手動配布"):
            c_f1, c_f2 = st.columns(2)
            with c_f1:
                if st.button("全員の「休み」を 1 にリセット", use_container_width=True):
                    msg = admin_force_grant_all("rest")
                    st.toast(msg); st.success(msg)
            with c_f2:
                if st.button("全員の「有給」を 2 にリセット", use_container_width=True):
                    msg = admin_force_grant_all("paid")
                    st.toast(msg); st.success(msg)
        st.divider()
        target_u = st.selectbox("対象者", ["(選択)"] + list(user_names.keys()), key="adm_u")
        if target_u != "(選択)":
            tid = user_names[target_u]
            with st.expander("① 運用開始前の罰金 (繰越) 設定"):
                current_init = users[users['id'].astype(str)==tid]['initial_fine'].iloc[0]
                with st.form(key=f"init_fine_form_{tid}"):
                    new_init = st.number_input("運用前罰金額", value=int(current_init), step=100)
                    if st.form_submit_button("保存"):
                        update_initial_fine(tid, new_init)
                        st.toast("保存しました"); st.success("保存しました"); t.sleep(3); st.rerun()
            with st.expander("② 休暇残数の個別修正"):
                with st.form(key=f"balance_form_{tid}", clear_on_submit=True):
                    c1, c2 = st.columns(2)
                    with c1: r = st.number_input("休み 増減", step=1)
                    with c2: p = st.number_input("有休 増減", step=1)
                    if st.form_submit_button("更新"):
                        if r != 0: update_user_balance(tid, "rest_balance", r)
                        if p != 0: update_user_balance(tid, "paid_leave_balance", p)
                        st.toast("更新しました"); st.success("更新しました"); t.sleep(3); st.rerun()
            with st.expander("③ 日別レコードの修正"):
                edit_date = st.date_input("修正する日付を選択", value=datetime.now(JST))
                conn = connect_to_gsheets()
                df_r = get_records_stable()
                edit_date_str = edit_date.strftime('%Y-%m-%d')
                rec = df_r[(df_r['user_id'].astype(str) == tid) & (df_r['date'] == edit_date_str)]
                if not rec.empty:
                    rec_row = rec.iloc[0]
                    rid = str(rec_row['id'])
                    st.info(f"現在: {rec_row['status']} | 罰金{rec_row['fine']}円")
                    with st.form("edit_record"):
                        mode = st.radio("修正モード", ["自動計算 (時刻から判定)", "「休み」に変更", "「有休」に変更"])
                        t_in_def = datetime.strptime(rec_row['clock_in'], '%H:%M:%S').time() if rec_row['clock_in'] and rec_row['clock_in'] != "-" else time(9,0)
                        t_out_def = datetime.strptime(rec_row['clock_out'], '%H:%M:%S').time() if rec_row['clock_out'] and rec_row['clock_out'] != "-" else time(15,0)
                        new_in_t = st.time_input("出勤時刻", value=t_in_def)
                        new_out_t = st.time_input("退勤時刻", value=t_out_def)
                        new_note = st.text_input("備考", value=rec_row['note'])
                        if st.form_submit_button("修正を実行"):
                            msg, m_type = admin_update_record(rid, edit_date, new_in_t, new_out_t, new_note, mode)
                            if m_type == "success": st.toast("修正完了！"); st.success(msg)
                            else: st.toast("修正完了 (要確認)"); st.warning(msg)
                            t.sleep(5); st.rerun()
                else: st.warning("記録なし")

if __name__ == '__main__':
    main()