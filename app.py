from flask import Flask, flash, render_template, redirect, url_for, session, request
from concurrent.futures import ThreadPoolExecutor
import os
import re
import sqlite3
import psycopg2
from psycopg2 import extras
import gspread
from google.oauth2.service_account import Credentials



app = Flask(__name__)
app.secret_key = 'Wha7is7hek3y4?'

# Create a single thread pool for the app
executor = ThreadPoolExecutor(max_workers=3)

# PostgreSQL connection string (set as an environment variable or config)
POSTGRES_CONNECTION_STRING = os.environ.get('POSTGRES_CONNECTION_STRING') or "postgresql://neondb_owner:WaeG1Jp6ODRE@ep-quiet-bonus-a5798w3c-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require" 

# Google Sheets setup
SERVICE_ACCOUNT_FILE = 'service_account.json'
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"]
SPREADSHEET_ID = "1nsmZ-YDsWiy9745MFr8D595Z0SCSBFUgvkoSsA17eYE"
credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
gc = gspread.authorize(credentials)
sheet = gc.open_by_key(SPREADSHEET_ID).sheet1  # First sheet

request = google.auth.transport.requests.Request()
credentials.refresh(request)
print("✅ Google Service Account key is valid. Token acquired.")
# Function to initialize the databases and create tables
def init_db():
    # SQLite initialization
    sqlite_conn = sqlite3.connect('app.db')
    sqlite_cursor = sqlite_conn.cursor()
    sqlite_cursor.execute('''CREATE TABLE IF NOT EXISTS leads (
                 id INTEGER PRIMARY KEY AUTOINCREMENT, 
                 name TEXT, 
                 surname TEXT, 
                 phone TEXT, 
                 id_number TEXT, 
                 salary_range TEXT, 
                 debt_review TEXT, 
                 reason_for_assistance TEXT, 
                 availability TEXT
                 )''')
    sqlite_conn.commit()
    sqlite_conn.close()

    # PostgreSQL initialization
    try:
        postgres_conn = psycopg2.connect(POSTGRES_CONNECTION_STRING)
        postgres_cursor = postgres_conn.cursor()
        postgres_cursor.execute('''CREATE TABLE IF NOT EXISTS leads (
                     id SERIAL PRIMARY KEY, 
                     name TEXT, 
                     surname TEXT, 
                     phone TEXT, 
                     id_number TEXT, 
                     salary_range TEXT, 
                     debt_review TEXT, 
                     reason_for_assistance TEXT, 
                     availability TEXT
                     )''')
        postgres_conn.commit()
        postgres_conn.close()
    except psycopg2.Error as e:
        print(f"PostgreSQL initialization error: {e}")


def save_to_postgres(step, response, lead_id):
    """Background function to save to PostgreSQL."""
    try:
        postgres_conn = psycopg2.connect(POSTGRES_CONNECTION_STRING)
        postgres_cursor = postgres_conn.cursor()

        if lead_id:
            postgres_cursor.execute(
                f'UPDATE leads SET {step} = %s WHERE id = %s',
                (response, lead_id)
            )
        else:
            postgres_cursor.execute(
                f'INSERT INTO leads ({step}) VALUES (%s)',
                (response,)
            )
            postgres_conn.commit()
            postgres_cursor.execute("SELECT lastval()")
            postgres_lead_id = postgres_cursor.fetchone()[0]
            session['lead_id'] = postgres_lead_id  # store in session

        postgres_conn.commit()
        postgres_conn.close()
    except psycopg2.Error as e:
        print(f"[PostgreSQL save error]: {e}")


def save_to_google_sheet(step, response, lead_id):


    all_records = sheet.get_all_records()
    found_row = None

    for idx, record in enumerate(all_records, start=2):
        if str(record.get("id")) == str(lead_id):
            found_row = idx
            break

    headers = sheet.row_values(1)
    if step not in headers:
        raise ValueError(f"Step '{step}' not found in sheet headers: {headers}")

    if found_row:
        col_index = headers.index(step) + 1
        sheet.update_cell(found_row, col_index, response)
    else:
        new_row = {"id": lead_id, step: response}
        row_data = [new_row.get(h, "") for h in headers]
        sheet.append_row(row_data)


def save_response(step, response):
    """Save to SQLite immediately, then PostgreSQL in background."""
    sqlite_conn = sqlite3.connect('app.db')
    sqlite_cursor = sqlite_conn.cursor()

    # Convert list to string if needed
    if isinstance(response, list):
        response = ', '.join(response)

    lead_id = session.get('lead_id')

    # If no lead_id in session, create a new row (new session or first time)
    if not lead_id:
        sqlite_cursor.execute(
            f'INSERT INTO leads ({step}) VALUES (?)',
            (response,)
        )
        lead_id = sqlite_cursor.lastrowid
        session['lead_id'] = lead_id
    else:
        # Same session → update the same row
        sqlite_cursor.execute(
            f'UPDATE leads SET {step} = ? WHERE id = ?',
            (response, lead_id)
        )

    sqlite_conn.commit()
    sqlite_conn.close()

    # Save to PostgreSQL in background
    executor.submit(save_to_postgres, step, response, lead_id)

    # Background append to Google Sheet
    executor.submit(save_new_entry_to_google_sheet, lead_id)

def sync_sqlite_to_google_sheet():

    scopes = ["https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file("service_account.json", scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open("Leads").sheet1

    # Get all rows from SQLite
    conn = sqlite3.connect('app.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM leads")
    rows = cursor.fetchall()
    col_names = [desc[0] for desc in cursor.description]
    conn.close()

    # Clear the sheet and write headers
    sheet.clear()
    sheet.append_row(col_names)

    # Write all rows
    for row in rows:
        sheet.append_row(list(row))


def save_new_entry_to_google_sheet(lead_id):
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file("service_account.json", scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open("Leads").sheet1

    # Get current headers
    headers = sheet.row_values(1)

    # Get the lead data from SQLite
    conn = sqlite3.connect('app.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM leads WHERE id = ?", (lead_id,))
    row_data = cursor.fetchone()
    col_names = [desc[0] for desc in cursor.description]
    conn.close()

    if not row_data:
        return  # Nothing to update

    # Convert the SQLite row into a dict {col_name: value}
    lead_dict = dict(zip(col_names, row_data))

    # Get all records from Google Sheet
    all_records = sheet.get_all_records()
    found_row = None

    for idx, record in enumerate(all_records, start=2):  # start=2 because headers are row 1
        if str(record.get("id")) == str(lead_id):
            found_row = idx
            break

    if found_row:
        # Update each column in the existing row
        for col_name, value in lead_dict.items():
            if col_name in headers:
                col_index = headers.index(col_name) + 1
                sheet.update_cell(found_row, col_index, value if value is not None else "")
    else:
        # Add new row
        new_row = [lead_dict.get(h, "") for h in headers]
        sheet.append_row(new_row)



def check_rejection_criteria():
    conn = sqlite3.connect('app.db')
    c = conn.cursor()
    
    lead_id = session.get('lead_id', None)

    # Make sure lead_id is present
    if not lead_id:
        conn.close()
        return False

    # Fetch lead responses
    try:
        c.execute('SELECT salary_range, reason_for_assistance, availability FROM leads WHERE id = ?', (lead_id,))
        lead = c.fetchone()
    except sqlite3.Error as e:
        flash(f"An error occurred while checking rejection criteria: {e}")
        conn.close()
        return False

    conn.close()

    if lead:
        salary_range, reason_for_assistance, availability = lead
        # Simplified rejection logic
        if (salary_range == "No" or reason_for_assistance == "No" or availability == 'Exploring but not sure I’m ready yet'):
            return True  # Rejection criteria met
    return False  # Rejection criteria not met

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/form', methods=['GET', 'POST'])
def form():
    if request.method == 'POST':
        step = request.form['step']
        response = request.form.getlist(step) if request.form.getlist(step) else request.form['response']

        # Validation logic for each field
        if step == 'name':
            if not response or len(response.strip()) == 0:
                flash("Name cannot be empty.")
                return redirect(url_for('form', next_step=int(request.form['next_step'])))
        elif step == 'surname':
            surname_pattern = r"^[A-Za-z]+(?: [A-Za-z]+)*$"
            if not re.match(surname_pattern, response.strip()):
                flash("Please enter a valid surname.")
                return redirect(url_for('form', next_step=int(request.form['next_step'])))
        elif step == 'phone':
            phone_pattern = r"^\+?\d{10,15}$"
            if not re.match(phone_pattern, response):
                flash("Please enter a valid phone number (10-15 digits, optionally with a leading +).")
                return redirect(url_for('form', next_step=int(request.form['next_step'])))
    
        elif step == 'id_number':
            id_pattern = r"^\d{13}$"  # exactly 13 digits
            if not re.match(id_pattern, response.strip()):
                flash("Please enter a valid 13-digit South African ID number.")
                return redirect(url_for('form', next_step=int(request.form['next_step'])))
        elif not response:
            flash("Please select an option.")
            return redirect(url_for('form', next_step=int(request.form['next_step'])))


        elif step in ['salary_range', 'debt_review', 'reason_for_assistance', 'availability']:
            if not response:
                flash("Please select an option.")
                return redirect(url_for('form', next_step=int(request.form['next_step'])))

        # Save the response if validation passes
        save_response(step, response)

        # Save name in session for personalization
        if step == 'name':
            session['user_name'] = response

        # Check for rejection criteria
        if check_rejection_criteria():
            return redirect(url_for('rejection'))

        # Redirect to the next step
        return redirect(url_for('form', next_step=int(request.form['next_step']) + 1))

    # Define the sequence of questions
    steps = [
        {'name': 'name', 'question': 'What is your name?'},
        {'name': 'surname', 'question': 'Kindly provide your surname.'},
        {'name': 'phone', 'question': 'What is your contact number? This will help us to reach out to you'},
        {'name': 'id_number', 'question': 'What is your ID Number (We will keep this information confidential)'},
        {'name': 'salary_range', 'question': 'What is your salary range?', 'options': ['Less than R5 000', "R5 000 - R8 000",'R8 000 - R12 000', "R12 000 - R15 000", 'R15 000 - R21 000', 'R20 000+']},
        {'name': 'debt_review', 'question': 'Are you under debt review?', 'options': ['Yes','No']},
        {'name': 'reason_for_assistance', 'question': '"Please select the reason why you need assistance?', 'options': ['Removal of Debt Review', 'Clearance', 'Reinstate under Debt Review', 'Summons/Judgement', 'Legal Action', 'Credit Score Rebuild', 'Loan']},
        {'name': 'availability', 'question': '"What is the best time we can call you?', 'options': ['08:00 AM – 10:00 AM', '10:00 AM – 12:00 PM', '12:00 PM – 14:00 PM', '14:00 PM – 16:00 PM', '16:00 PM – 18:00 PM']},
    ]
    # Determine which step to show next
    next_step = int(request.args.get('next_step', 0))
    if next_step >= len(steps):
        if check_rejection_criteria():
           return redirect(url_for('rejection'))
        return redirect(url_for('instructions'))

    step = steps[next_step]

    # Calculate progress percentage
    progress_percentage = (next_step / len(steps)) * 100

    # Get user name for personalization
    user_name = session.get('user_name', None)

    return render_template('form.html', step=step, next_step=next_step, total_steps=len(steps), progress_percentage=progress_percentage, user_name=user_name)

# Rejection page
@app.route('/rejection')
def rejection():
    return render_template('rejection.html')

# Thank you page after form completion 
@app.route('/thank-you')
def thank_you():
    return render_template('thank_you.html')

@app.route('/instructions')
def instructions():
    return render_template('instructions.html')

@app.route('/responses')
def view_responses():
    try:
        conn = sqlite3.connect('app.db')
        c = conn.cursor()
        c.execute("SELECT * FROM leads")
        responses = c.fetchall()
        conn.close()
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        responses = []
    return render_template('responses.html', responses=responses)

@app.route('/privacy')
def privacy():
    return render_template('privacy.html')

def startup_sync():
    print("Syncing all SQLite leads to Google Sheets...")
    try:
        sync_sqlite_to_google_sheet()
        print("✅ Google Sheets sync complete.")
    except Exception as e:
        print(f"❌ Google Sheets sync failed: {e}")


if __name__ == "__main__":
    init_db()
    startup_sync()
    app.run(debug=True, port=5000)