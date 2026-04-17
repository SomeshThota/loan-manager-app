import os
import sqlite3
from datetime import date, datetime

from flask import Flask, flash, redirect, render_template, request, url_for

app = Flask(__name__)
app.secret_key = "change-this-secret-key"

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATABASE = os.path.join(BASE_DIR, "finance.db")


def get_db_connection():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT,
            notes TEXT,
            created_at TEXT NOT NULL,
            reminder_date TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS loan_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            principal REAL NOT NULL,
            monthly_rate REAL NOT NULL DEFAULT 2.0,
            given_date TEXT NOT NULL,
            due_date TEXT,
            notes TEXT,
            FOREIGN KEY (client_id) REFERENCES clients (id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            payment_date TEXT NOT NULL,
            amount REAL NOT NULL,
            alloc_type TEXT NOT NULL,
            alloc_interest REAL NOT NULL,
            alloc_principal REAL NOT NULL,
            notes TEXT,
            FOREIGN KEY (client_id) REFERENCES clients (id)
        )
        """
    )

    def table_columns(table_name):
        cur.execute(f"PRAGMA table_info({table_name})")
        return {row[1] for row in cur.fetchall()}

    def recreate_if_incompatible(table_name, required_columns, create_sql):
        existing = table_columns(table_name)
        if not existing:
            return
        if required_columns.issubset(existing):
            return

        # Old schema detected from previous versions; recreate clean table.
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute(create_sql)

    recreate_if_incompatible(
        "clients",
        {"id", "name", "phone", "notes", "created_at", "reminder_date"},
        """
        CREATE TABLE clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT,
            notes TEXT,
            created_at TEXT NOT NULL,
            reminder_date TEXT
        )
        """,
    )
    recreate_if_incompatible(
        "loan_entries",
        {"id", "client_id", "principal", "monthly_rate", "given_date", "due_date", "notes"},
        """
        CREATE TABLE loan_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            principal REAL NOT NULL,
            monthly_rate REAL NOT NULL DEFAULT 2.0,
            given_date TEXT NOT NULL,
            due_date TEXT,
            notes TEXT,
            FOREIGN KEY (client_id) REFERENCES clients (id)
        )
        """,
    )
    recreate_if_incompatible(
        "payments",
        {"id", "client_id", "payment_date", "amount", "alloc_type", "alloc_interest", "alloc_principal", "notes"},
        """
        CREATE TABLE payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            payment_date TEXT NOT NULL,
            amount REAL NOT NULL,
            alloc_type TEXT NOT NULL,
            alloc_interest REAL NOT NULL,
            alloc_principal REAL NOT NULL,
            notes TEXT,
            FOREIGN KEY (client_id) REFERENCES clients (id)
        )
        """,
    )
    conn.commit()
    conn.close()


def parse_date(value, default_today=False):
    if not value:
        return date.today() if default_today else None
    return datetime.strptime(value, "%Y-%m-%d").date()


def months_from_days(start_date: date, end_date: date) -> float:
    if end_date <= start_date:
        return 0.0
    days = (end_date - start_date).days
    return days / 30.0


def compute_interest(principal: float, monthly_rate: float, start_date: date, calc_date: date) -> float:
    months = months_from_days(start_date, calc_date)
    return principal * (monthly_rate / 100.0) * months


def get_client_or_404(conn, client_id):
    cur = conn.cursor()
    cur.execute("SELECT * FROM clients WHERE id = ?", (client_id,))
    row = cur.fetchone()
    return row


def compute_client_snapshot(conn, client_id: int, calc_date: date, exclude_payment_id: int | None = None):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM loan_entries
        WHERE client_id = ?
        ORDER BY given_date ASC, id ASC
        """,
        (client_id,),
    )
    loans = cur.fetchall()

    if exclude_payment_id is None:
        cur.execute(
            """
            SELECT * FROM payments
            WHERE client_id = ?
            ORDER BY payment_date ASC, id ASC
            """,
            (client_id,),
        )
    else:
        cur.execute(
            """
            SELECT * FROM payments
            WHERE client_id = ? AND id != ?
            ORDER BY payment_date ASC, id ASC
            """,
            (client_id, exclude_payment_id),
        )
    payments = cur.fetchall()

    loan_breakdown = []
    total_principal = 0.0
    total_interest = 0.0

    for loan in loans:
        principal = float(loan["principal"])
        rate = float(loan["monthly_rate"])
        start = parse_date(loan["given_date"])
        interest = compute_interest(principal, rate, start, calc_date)
        total_principal += principal
        total_interest += interest
        loan_breakdown.append(
            {
                "loan": loan,
                "interest": interest,
                "months_elapsed": months_from_days(start, calc_date),
            }
        )

    paid_principal = sum(float(p["alloc_principal"] or 0) for p in payments)
    paid_interest = sum(float(p["alloc_interest"] or 0) for p in payments)
    total_paid = sum(float(p["amount"] or 0) for p in payments)

    principal_outstanding = max(total_principal - paid_principal, 0.0)
    interest_outstanding = max(total_interest - paid_interest, 0.0)
    total_outstanding = principal_outstanding + interest_outstanding

    last_payment_date = payments[-1]["payment_date"] if payments else None

    overdue_loan_count = 0
    for item in loan_breakdown:
        loan_due = item["loan"]["due_date"]
        if loan_due:
            due_date = parse_date(loan_due)
            if due_date and due_date < calc_date and total_outstanding > 0.01:
                overdue_loan_count += 1

    return {
        "loans": loans,
        "payments": payments,
        "loan_count": len(loans),
        "payment_count": len(payments),
        "loan_breakdown": loan_breakdown,
        "total_principal": total_principal,
        "total_interest": total_interest,
        "total_due": total_principal + total_interest,
        "paid_principal": paid_principal,
        "paid_interest": paid_interest,
        "total_paid": total_paid,
        "principal_outstanding": principal_outstanding,
        "interest_outstanding": interest_outstanding,
        "total_outstanding": total_outstanding,
        "last_payment_date": last_payment_date,
        "overdue_loan_count": overdue_loan_count,
    }


def get_auto_allocation(conn, client_id: int, payment_date: date, amount: float, exclude_payment_id: int | None = None):
    snapshot = compute_client_snapshot(conn, client_id, payment_date, exclude_payment_id=exclude_payment_id)
    interest_due = snapshot["interest_outstanding"]
    principal_due = snapshot["principal_outstanding"]

    alloc_interest = min(amount, interest_due)
    remaining = amount - alloc_interest
    alloc_principal = min(remaining, principal_due)
    return alloc_interest, alloc_principal


@app.route("/")
def home():
    return redirect(url_for("clients"))


@app.route("/clients", methods=["GET", "POST"])
def clients():
    conn = get_db_connection()
    cur = conn.cursor()

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        phone = (request.form.get("phone") or "").strip()
        notes = (request.form.get("notes") or "").strip()
        reminder_date = (request.form.get("reminder_date") or "").strip()

        if not name:
            flash("Client name is required.", "danger")
            conn.close()
            return redirect(url_for("clients"))

        cur.execute(
            """
            INSERT INTO clients (name, phone, notes, created_at, reminder_date)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                name,
                phone or None,
                notes or None,
                date.today().isoformat(),
                reminder_date or None,
            ),
        )
        conn.commit()
        flash("Client added.", "success")
        conn.close()
        return redirect(url_for("clients"))

    search = (request.args.get("search") or "").strip()
    status = (request.args.get("status") or "all").strip().lower()
    if search:
        cur.execute(
            """
            SELECT * FROM clients
            WHERE name LIKE ?
            ORDER BY name ASC
            """,
            (f"%{search}%",),
        )
    else:
        cur.execute("SELECT * FROM clients ORDER BY name ASC")
    clients_rows = cur.fetchall()

    today = date.today()
    clients_info = []
    for c in clients_rows:
        snapshot = compute_client_snapshot(conn, c["id"], today)
        reminder_date = parse_date(c["reminder_date"]) if c["reminder_date"] else None
        reminder_due = bool(reminder_date and reminder_date <= today and snapshot["total_outstanding"] > 0.01)
        is_pending = snapshot["total_outstanding"] > 0.01
        is_clear = not is_pending
        is_reminder = reminder_due or snapshot["overdue_loan_count"] > 0

        include = True
        if status == "pending":
            include = is_pending
        elif status == "clear":
            include = is_clear
        elif status == "reminder":
            include = is_reminder

        if include:
            clients_info.append(
                {
                    "client": c,
                    "snapshot": snapshot,
                    "reminder_due": reminder_due,
                }
            )

    conn.close()
    return render_template("clients.html", clients_info=clients_info, search=search, status=status, today=today)


@app.route("/client/<int:client_id>/edit", methods=["GET", "POST"])
def edit_client(client_id):
    conn = get_db_connection()
    client = get_client_or_404(conn, client_id)
    if client is None:
        conn.close()
        flash("Client not found.", "warning")
        return redirect(url_for("clients"))

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        phone = (request.form.get("phone") or "").strip()
        notes = (request.form.get("notes") or "").strip()
        reminder_date = (request.form.get("reminder_date") or "").strip()
        if not name:
            flash("Client name is required.", "danger")
            conn.close()
            return redirect(url_for("edit_client", client_id=client_id))

        cur = conn.cursor()
        cur.execute(
            """
            UPDATE clients
            SET name = ?, phone = ?, notes = ?, reminder_date = ?
            WHERE id = ?
            """,
            (name, phone or None, notes or None, reminder_date or None, client_id),
        )
        conn.commit()
        conn.close()
        flash("Client details updated.", "success")
        return redirect(url_for("client_detail", client_id=client_id))

    conn.close()
    return render_template("edit_client.html", client=client)


@app.route("/client/<int:client_id>/delete", methods=["POST"])
def delete_client(client_id):
    conn = get_db_connection()
    client = get_client_or_404(conn, client_id)
    if client is None:
        conn.close()
        flash("Client not found.", "warning")
        return redirect(url_for("clients"))

    cur = conn.cursor()
    cur.execute("DELETE FROM payments WHERE client_id = ?", (client_id,))
    cur.execute("DELETE FROM loan_entries WHERE client_id = ?", (client_id,))
    cur.execute("DELETE FROM clients WHERE id = ?", (client_id,))
    conn.commit()
    conn.close()
    flash("Client deleted.", "info")
    return redirect(url_for("clients"))


@app.route("/client/<int:client_id>")
def client_detail(client_id):
    conn = get_db_connection()
    client = get_client_or_404(conn, client_id)
    if client is None:
        conn.close()
        flash("Client not found.", "warning")
        return redirect(url_for("clients"))

    calc_date_raw = (request.args.get("calc_date") or "").strip()
    try:
        calc_date = parse_date(calc_date_raw, default_today=True)
    except ValueError:
        calc_date = date.today()

    snapshot = compute_client_snapshot(conn, client_id, calc_date)
    conn.close()
    return render_template(
        "client_detail.html",
        client=client,
        snapshot=snapshot,
        calc_date=calc_date.isoformat(),
    )


@app.route("/client/<int:client_id>/loan/add", methods=["POST"])
def add_loan_entry(client_id):
    conn = get_db_connection()
    client = get_client_or_404(conn, client_id)
    if client is None:
        conn.close()
        flash("Client not found.", "warning")
        return redirect(url_for("clients"))

    principal_raw = request.form.get("principal")
    rate_raw = request.form.get("monthly_rate")
    given_date_raw = request.form.get("given_date")
    due_date_raw = request.form.get("due_date")
    notes = (request.form.get("notes") or "").strip()

    try:
        principal = float(principal_raw)
        monthly_rate = float(rate_raw)
    except (TypeError, ValueError):
        flash("Enter valid principal and rate.", "danger")
        conn.close()
        return redirect(url_for("client_detail", client_id=client_id))

    if principal <= 0:
        flash("Principal must be greater than 0.", "danger")
        conn.close()
        return redirect(url_for("client_detail", client_id=client_id))
    if monthly_rate < 0:
        flash("Interest rate cannot be negative.", "danger")
        conn.close()
        return redirect(url_for("client_detail", client_id=client_id))

    try:
        given_date = parse_date(given_date_raw, default_today=True)
        due_date = parse_date(due_date_raw) if due_date_raw else None
    except ValueError:
        flash("Enter valid dates.", "danger")
        conn.close()
        return redirect(url_for("client_detail", client_id=client_id))

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO loan_entries (client_id, principal, monthly_rate, given_date, due_date, notes)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            client_id,
            principal,
            monthly_rate,
            given_date.isoformat(),
            due_date.isoformat() if due_date else None,
            notes or None,
        ),
    )
    conn.commit()
    conn.close()
    flash("Loan entry added.", "success")
    return redirect(url_for("client_detail", client_id=client_id))


@app.route("/loan/<int:loan_id>/edit", methods=["GET", "POST"])
def edit_loan_entry(loan_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM loan_entries WHERE id = ?", (loan_id,))
    loan = cur.fetchone()
    if loan is None:
        conn.close()
        flash("Loan entry not found.", "warning")
        return redirect(url_for("clients"))

    if request.method == "POST":
        principal_raw = request.form.get("principal")
        rate_raw = request.form.get("monthly_rate")
        given_date_raw = request.form.get("given_date")
        due_date_raw = request.form.get("due_date")
        notes = (request.form.get("notes") or "").strip()
        try:
            principal = float(principal_raw)
            monthly_rate = float(rate_raw)
            given_date = parse_date(given_date_raw, default_today=True)
            due_date = parse_date(due_date_raw) if due_date_raw else None
        except (TypeError, ValueError):
            flash("Please enter valid values.", "danger")
            conn.close()
            return redirect(url_for("edit_loan_entry", loan_id=loan_id))
        if principal <= 0:
            flash("Principal must be greater than 0.", "danger")
            conn.close()
            return redirect(url_for("edit_loan_entry", loan_id=loan_id))
        if monthly_rate < 0:
            flash("Interest rate cannot be negative.", "danger")
            conn.close()
            return redirect(url_for("edit_loan_entry", loan_id=loan_id))

        cur.execute(
            """
            UPDATE loan_entries
            SET principal = ?, monthly_rate = ?, given_date = ?, due_date = ?, notes = ?
            WHERE id = ?
            """,
            (
                principal,
                monthly_rate,
                given_date.isoformat(),
                due_date.isoformat() if due_date else None,
                notes or None,
                loan_id,
            ),
        )
        conn.commit()
        client_id = loan["client_id"]
        conn.close()
        flash("Loan entry updated.", "success")
        return redirect(url_for("client_detail", client_id=client_id))

    conn.close()
    return render_template("edit_loan.html", loan=loan)


@app.route("/loan/<int:loan_id>/delete", methods=["POST"])
def delete_loan_entry(loan_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT client_id FROM loan_entries WHERE id = ?", (loan_id,))
    row = cur.fetchone()
    if row is None:
        conn.close()
        flash("Loan entry not found.", "warning")
        return redirect(url_for("clients"))
    client_id = row["client_id"]
    cur.execute("DELETE FROM loan_entries WHERE id = ?", (loan_id,))
    conn.commit()
    conn.close()
    flash("Loan entry deleted.", "info")
    return redirect(url_for("client_detail", client_id=client_id))


@app.route("/client/<int:client_id>/payment/add", methods=["POST"])
def add_payment(client_id):
    conn = get_db_connection()
    client = get_client_or_404(conn, client_id)
    if client is None:
        conn.close()
        flash("Client not found.", "warning")
        return redirect(url_for("clients"))

    payment_date_raw = request.form.get("payment_date")
    amount_raw = request.form.get("amount")
    alloc_type = (request.form.get("alloc_type") or "auto").strip()
    alloc_interest_raw = request.form.get("alloc_interest")
    alloc_principal_raw = request.form.get("alloc_principal")
    notes = (request.form.get("notes") or "").strip()

    try:
        payment_date = parse_date(payment_date_raw, default_today=True)
        amount = float(amount_raw)
    except (TypeError, ValueError):
        flash("Enter valid payment date and amount.", "danger")
        conn.close()
        return redirect(url_for("client_detail", client_id=client_id))

    if amount <= 0:
        flash("Payment amount must be greater than 0.", "danger")
        conn.close()
        return redirect(url_for("client_detail", client_id=client_id))

    alloc_interest = 0.0
    alloc_principal = 0.0

    if alloc_type == "interest_only":
        alloc_interest = amount
    elif alloc_type == "principal_only":
        alloc_principal = amount
    elif alloc_type == "split_custom":
        try:
            alloc_interest = float(alloc_interest_raw or 0)
            alloc_principal = float(alloc_principal_raw or 0)
        except ValueError:
            flash("Enter valid split values.", "danger")
            conn.close()
            return redirect(url_for("client_detail", client_id=client_id))
    else:
        alloc_type = "auto"
        alloc_interest, alloc_principal = get_auto_allocation(conn, client_id, payment_date, amount)

    if alloc_interest < 0 or alloc_principal < 0:
        flash("Allocation values cannot be negative.", "danger")
        conn.close()
        return redirect(url_for("client_detail", client_id=client_id))

    if round(alloc_interest + alloc_principal, 2) > round(amount, 2):
        flash("Allocation exceeds payment amount.", "danger")
        conn.close()
        return redirect(url_for("client_detail", client_id=client_id))

    snapshot_at_payment = compute_client_snapshot(conn, client_id, payment_date)
    if alloc_interest > snapshot_at_payment["interest_outstanding"] + 0.01:
        flash("Interest allocation exceeds interest outstanding.", "danger")
        conn.close()
        return redirect(url_for("client_detail", client_id=client_id))
    if alloc_principal > snapshot_at_payment["principal_outstanding"] + 0.01:
        flash("Principal allocation exceeds principal outstanding.", "danger")
        conn.close()
        return redirect(url_for("client_detail", client_id=client_id))

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO payments (
            client_id, payment_date, amount, alloc_type, alloc_interest, alloc_principal, notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            client_id,
            payment_date.isoformat(),
            amount,
            alloc_type,
            alloc_interest,
            alloc_principal,
            notes or None,
        ),
    )
    conn.commit()
    conn.close()
    flash("Payment added.", "success")
    return redirect(url_for("client_detail", client_id=client_id))


@app.route("/payment/<int:payment_id>/edit", methods=["GET", "POST"])
def edit_payment(payment_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM payments WHERE id = ?", (payment_id,))
    payment = cur.fetchone()
    if payment is None:
        conn.close()
        flash("Payment not found.", "warning")
        return redirect(url_for("clients"))

    client_id = payment["client_id"]

    if request.method == "POST":
        payment_date_raw = request.form.get("payment_date")
        amount_raw = request.form.get("amount")
        alloc_type = (request.form.get("alloc_type") or "auto").strip()
        alloc_interest_raw = request.form.get("alloc_interest")
        alloc_principal_raw = request.form.get("alloc_principal")
        notes = (request.form.get("notes") or "").strip()

        try:
            payment_date = parse_date(payment_date_raw, default_today=True)
            amount = float(amount_raw)
        except (TypeError, ValueError):
            flash("Enter valid payment date and amount.", "danger")
            conn.close()
            return redirect(url_for("edit_payment", payment_id=payment_id))

        if amount <= 0:
            flash("Payment amount must be greater than 0.", "danger")
            conn.close()
            return redirect(url_for("edit_payment", payment_id=payment_id))

        alloc_interest = 0.0
        alloc_principal = 0.0

        if alloc_type == "interest_only":
            alloc_interest = amount
        elif alloc_type == "principal_only":
            alloc_principal = amount
        elif alloc_type == "split_custom":
            try:
                alloc_interest = float(alloc_interest_raw or 0)
                alloc_principal = float(alloc_principal_raw or 0)
            except ValueError:
                flash("Enter valid split values.", "danger")
                conn.close()
                return redirect(url_for("edit_payment", payment_id=payment_id))
        else:
            alloc_type = "auto"
            alloc_interest, alloc_principal = get_auto_allocation(
                conn,
                client_id,
                payment_date,
                amount,
                exclude_payment_id=payment_id,
            )

        if round(alloc_interest + alloc_principal, 2) > round(amount, 2):
            flash("Allocation exceeds payment amount.", "danger")
            conn.close()
            return redirect(url_for("edit_payment", payment_id=payment_id))

        snapshot_at_payment = compute_client_snapshot(
            conn, client_id, payment_date, exclude_payment_id=payment_id
        )
        if alloc_interest > snapshot_at_payment["interest_outstanding"] + 0.01:
            flash("Interest allocation exceeds interest outstanding.", "danger")
            conn.close()
            return redirect(url_for("edit_payment", payment_id=payment_id))
        if alloc_principal > snapshot_at_payment["principal_outstanding"] + 0.01:
            flash("Principal allocation exceeds principal outstanding.", "danger")
            conn.close()
            return redirect(url_for("edit_payment", payment_id=payment_id))

        cur.execute(
            """
            UPDATE payments
            SET payment_date = ?, amount = ?, alloc_type = ?, alloc_interest = ?, alloc_principal = ?, notes = ?
            WHERE id = ?
            """,
            (
                payment_date.isoformat(),
                amount,
                alloc_type,
                alloc_interest,
                alloc_principal,
                notes or None,
                payment_id,
            ),
        )
        conn.commit()
        conn.close()
        flash("Payment updated.", "success")
        return redirect(url_for("client_detail", client_id=client_id))

    conn.close()
    return render_template("edit_payment.html", payment=payment)


@app.route("/payment/<int:payment_id>/delete", methods=["POST"])
def delete_payment(payment_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT client_id FROM payments WHERE id = ?", (payment_id,))
    row = cur.fetchone()
    if row is None:
        conn.close()
        flash("Payment not found.", "warning")
        return redirect(url_for("clients"))
    client_id = row["client_id"]
    cur.execute("DELETE FROM payments WHERE id = ?", (payment_id,))
    conn.commit()
    conn.close()
    flash("Payment deleted.", "info")
    return redirect(url_for("client_detail", client_id=client_id))


if __name__ == "__main__":
    init_db()
    app.run(debug=True)

