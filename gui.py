# poc_tk_sqlite_hierarchical.py
import sqlite3 as s3
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import pandas as pd
import os

DB = "poc.db"

# ---------- DB ----------
def g():
    c = s3.connect(DB)
    c.execute("PRAGMA foreign_keys = ON")
    return c

def init_db():
    c = g(); cur = c.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS author (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            email TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS book (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            year INTEGER,
            author_id INTEGER NOT NULL,
            FOREIGN KEY(author_id) REFERENCES author(id) ON DELETE CASCADE
        )
    """)
    c.commit(); c.close()

# ---------- UI refresh ----------
def r_authors():
    c = g(); cur = c.cursor()
    cur.execute("SELECT id, name FROM author ORDER BY name")
    rows = cur.fetchall(); c.close()
    vals = [f"{r[0]} - {r[1]}" for r in rows]
    cmb_author['values'] = vals
    lb_authors.delete(0, tk.END)
    for v in vals:
        lb_authors.insert(tk.END, v)

def r_books():
    c = g(); cur = c.cursor()
    cur.execute("""
        SELECT book.id, book.title, book.year, author.name
        FROM book JOIN author ON book.author_id = author.id
        ORDER BY book.id DESC
    """)
    rows = cur.fetchall(); c.close()
    tv_books.delete(*tv_books.get_children())
    for r in rows:
        tv_books.insert("", tk.END, values=r)

# ---------- single-row insert ----------
def add_author_ui():
    n = ent_author_name.get().strip(); e = ent_author_email.get().strip()
    if not n:
        messagebox.showerror("Validation", "Author name required."); return
    c = g(); cur = c.cursor()
    try:
        cur.execute("INSERT OR IGNORE INTO author(name, email) VALUES (?, ?)", (n, e or None))
        if cur.rowcount == 0 and e:
            cur.execute("UPDATE author SET email=? WHERE name=?", (e, n))
        c.commit()
    except Exception as ex:
        messagebox.showerror("DB Error", str(ex))
    finally:
        c.close()
    ent_author_name.delete(0, tk.END); ent_author_email.delete(0, tk.END)
    r_authors(); messagebox.showinfo("Saved", f"Author '{n}' added/updated.")

def add_book_ui():
    t = ent_book_title.get().strip(); y = ent_book_year.get().strip(); a = cmb_author.get().strip()
    if not t:
        messagebox.showerror("Validation", "Book title required."); return
    if not a:
        messagebox.showerror("Validation", "Select an author."); return
    try:
        aid = int(a.split(" - ")[0])
    except Exception:
        messagebox.showerror("Validation", "Invalid author selection."); return
    try:
        yv = int(y) if y else None
    except Exception:
        messagebox.showerror("Validation", "Year must be a number."); return
    c = g(); cur = c.cursor()
    try:
        cur.execute("INSERT INTO book(title, year, author_id) VALUES (?, ?, ?)", (t, yv, aid))
        c.commit()
    except Exception as ex:
        messagebox.showerror("DB Error", str(ex))
    finally:
        c.close()
    ent_book_title.delete(0, tk.END); ent_book_year.delete(0, tk.END)
    r_books(); messagebox.showinfo("Saved", f"Book '{t}' added.")

# ---------- helpers for upload ----------
def get_or_create_author_by_name(nm):
    n = str(nm).strip()
    if not n or n.lower() == "nan": return None
    c = g(); cur = c.cursor()
    cur.execute("SELECT id FROM author WHERE name = ?", (n,))
    r = cur.fetchone()
    if r:
        aid = r[0]; c.close(); return aid
    try:
        cur.execute("INSERT INTO author(name, email) VALUES (?, ?)", (n, None))
        c.commit()
        aid = cur.lastrowid
    except Exception:
        cur.execute("SELECT id FROM author WHERE name = ?", (n,))
        r2 = cur.fetchone(); aid = r2[0] if r2 else None
    c.close(); return aid

def parse_key_value_pairs_from_df(df):
    """
    Accepts a dataframe read from the sheet (no header).
    Supports:
    - two-column rows: key in col0, value in col1
    - single-column vertical pairs: key row followed by value row
    Returns list of (key, value) pairs in order.
    """
    pairs = []
    # normalize columns to strings
    # handle if df has >=2 columns and there are many non-nulls in col1 => prefer same-row pairs
    if df.shape[1] >= 2 and df.iloc[:,1].notna().any():
        for _, r in df.iterrows():
            k = r.iloc[0]; v = r.iloc[1]
            if pd.isna(k): continue
            if pd.isna(v):
                # if value missing in col1, try to take next non-null row in col0 as value (vertical style)
                continue
            pairs.append((str(k).strip(), str(v).strip()))
        # Also detect single-column vertical entries if above missed some
        # (fall through later if needed)
    else:
        # single-column or second column empty -> interpret as vertical pairs
        col0 = df.iloc[:,0].tolist()
        i = 0
        n = len(col0)
        while i < n:
            k = col0[i]
            if pd.isna(k) or str(k).strip() == "":
                i += 1; continue
            kstr = str(k).strip()
            # find next non-empty value row
            v = None
            j = i+1
            while j < n:
                cand = col0[j]
                if not pd.isna(cand) and str(cand).strip() != "":
                    v = str(cand).strip(); break
                j += 1
            if v is not None:
                pairs.append((kstr, v))
                i = j+1
            else:
                # no value found, skip
                i += 1
    return pairs

def parse_hierarchical_pairs(pairs):
    """
    Given list of (key, value) pairs like ('author>name','Alice'), ('book>title','X')
    Produce lists of author dicts and book dicts.
    Heuristic: when encountering an 'author>name' while a current_author exists -> start new author.
               when encountering a 'book>title' while current_book exists -> start new book.
    """
    authors = []
    books = []
    cur_a = {}
    cur_b = {}
    for k, v in pairs:
        if ">" not in k: continue
        ent, fld = [x.strip() for x in k.split(">", 1)]
        if ent.lower() == "author":
            # start new author if we see name and cur_a already has data
            if fld.lower() == "name" and cur_a:
                authors.append(cur_a); cur_a = {}
            cur_a[fld.lower()] = v
        elif ent.lower() == "book":
            if fld.lower() == "title" and cur_b:
                books.append(cur_b); cur_b = {}
            cur_b[fld.lower()] = v
        else:
            # ignore unknown prefixes for now
            pass
    if cur_a: authors.append(cur_a)
    if cur_b: books.append(cur_b)
    return authors, books

# ---------- upload hierarchical Excel ----------
def upload_hierarchical():
    fp = filedialog.askopenfilename(title="Select hierarchical Excel", filetypes=[("Excel files","*.xlsx *.xls")])
    if not fp: return
    if not os.path.exists(fp):
        messagebox.showerror("File Error", "Selected file does not exist."); return

    msgs = []; ia = 0; ib = 0; sk = 0
    try:
        xls = pd.ExcelFile(fp)
    except Exception as ex:
        messagebox.showerror("Read Error", f"Failed to open Excel: {ex}"); return

    # We'll parse the first sheet by default (user's hierarchical sheet)
    sheet = xls.sheet_names[0]
    try:
        df = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=object)
    except Exception as ex:
        messagebox.showerror("Read Error", f"Failed to read sheet '{sheet}': {ex}"); return

    pairs = parse_key_value_pairs_from_df(df)
    if not pairs:
        messagebox.showerror("Parse Error", "No key-value pairs detected in the sheet."); return

    authors_list, books_list = parse_hierarchical_pairs(pairs)

    c = g(); cur = c.cursor()

    # insert authors
    for idx, a in enumerate(authors_list):
        name = a.get("name") or a.get("author>name") or a.get("author_name")
        email = a.get("email") or a.get("author>email")
        if not name or str(name).strip().lower() == "nan":
            sk += 1; msgs.append(f"Author block {idx+1} skipped: missing name"); continue
        n = str(name).strip(); e = None if email is None or pd.isna(email) else str(email).strip()
        try:
            cur.execute("INSERT OR IGNORE INTO author(name, email) VALUES (?, ?)", (n, e))
            if cur.rowcount == 0 and e:
                cur.execute("UPDATE author SET email=? WHERE name=?", (e, n))
            ia += 1
        except Exception as ex:
            sk += 1; msgs.append(f"Author block {idx+1} skipped: {ex}")

    # insert books
    for idx, b in enumerate(books_list):
        title = b.get("title") or b.get("book>title")
        year = b.get("year") or b.get("book>year")
        ba = b.get("author") or b.get("book>author")
        if not title or str(title).strip().lower() == "nan":
            sk += 1; msgs.append(f"Book block {idx+1} skipped: missing title"); continue
        if not ba or str(ba).strip().lower() == "nan":
            sk += 1; msgs.append(f"Book block {idx+1} skipped: missing book>author"); continue
        t = str(title).strip()
        try:
            yv = int(year) if (year is not None and not pd.isna(year) and str(year).strip() != "") else None
        except Exception:
            yv = None
        # locate or create author by name
        try:
            aid = get_or_create_author_by_name(ba)
            if aid is None:
                sk += 1; msgs.append(f"Book block {idx+1} skipped: invalid author '{ba}'"); continue
        except Exception as ex:
            sk += 1; msgs.append(f"Book block {idx+1} skipped: failed create/find author '{ba}' ({ex})"); continue
        try:
            cur.execute("INSERT INTO book(title, year, author_id) VALUES (?, ?, ?)", (t, yv, aid))
            ib += 1
        except Exception as ex:
            sk += 1; msgs.append(f"Book block {idx+1} skipped: {ex}")

    c.commit(); c.close()
    r_authors(); r_books()

    summary = f"Upload complete — authors processed: {ia}, books inserted: {ib}, rows skipped: {sk}."
    if msgs:
        detail = "\n".join(msgs[:12])
        if len(msgs) > 12:
            detail += f"\n...and {len(msgs)-12} more."
        messagebox.showinfo("Upload result", summary + "\n\nDetails:\n" + detail)
    else:
        messagebox.showinfo("Upload result", summary)

# ---------- build UI ----------
init_db()
root = tk.Tk()
root.title("POC: Tkinter + SQLite (hierarchical Excel upload)")
root.geometry("920x560")

# Author
fa = ttk.LabelFrame(root, text="Author"); fa.pack(fill="x", padx=10, pady=6)
ttk.Label(fa, text="Name").grid(row=0, column=0, padx=6, pady=6, sticky="w")
ent_author_name = ttk.Entry(fa, width=30); ent_author_name.grid(row=0, column=1, padx=6, pady=6, sticky="w")
ttk.Label(fa, text="Email").grid(row=0, column=2, padx=6, pady=6, sticky="w")
ent_author_email = ttk.Entry(fa, width=30); ent_author_email.grid(row=0, column=3, padx=6, pady=6, sticky="w")
btn_add_author = ttk.Button(fa, text="Add Author", command=add_author_ui); btn_add_author.grid(row=0, column=4, padx=6, pady=6)
lb_authors = tk.Listbox(fa, height=4); lb_authors.grid(row=1, column=0, columnspan=4, padx=6, pady=(0,8), sticky="we")
ttk.Label(fa, text="(id - name)").grid(row=1, column=4, padx=6, sticky="w")

# Book
fb = ttk.LabelFrame(root, text="Book"); fb.pack(fill="x", padx=10, pady=6)
ttk.Label(fb, text="Title").grid(row=0, column=0, padx=6, pady=6, sticky="w")
ent_book_title = ttk.Entry(fb, width=30); ent_book_title.grid(row=0, column=1, padx=6, pady=6, sticky="w")
ttk.Label(fb, text="Year").grid(row=0, column=2, padx=6, pady=6, sticky="w")
ent_book_year = ttk.Entry(fb, width=10); ent_book_year.grid(row=0, column=3, padx=6, pady=6, sticky="w")
ttk.Label(fb, text="Author").grid(row=0, column=4, padx=6, pady=6, sticky="w")
cmb_author = ttk.Combobox(fb, state="readonly", width=30); cmb_author.grid(row=0, column=5, padx=6, pady=6, sticky="w")
btn_add_book = ttk.Button(fb, text="Add Book", command=add_book_ui); btn_add_book.grid(row=0, column=6, padx=6, pady=6)

# Upload frame
fu = ttk.Frame(root); fu.pack(fill="x", padx=10, pady=6)
ttk.Label(fu, text="Bulk actions:").grid(row=0, column=0, padx=6, sticky="w")
btn_upload1 = ttk.Button(fu, text="Upload hierarchical Excel (key-value)", command=upload_hierarchical); btn_upload1.grid(row=0, column=1, padx=6, sticky="w")
ttk.Label(fu, text="Sheet format: key-value pairs. Keys like author>name, author>email, book>title, book>year, book>author").grid(row=0, column=2, padx=12, sticky="w")

# Books treeview
ft = ttk.LabelFrame(root, text="Saved Books"); ft.pack(fill="both", expand=True, padx=10, pady=6)
cols = ("id", "title", "year", "author")
tv_books = ttk.Treeview(ft, columns=cols, show="headings", height=14)
tv_books.heading("id", text="ID"); tv_books.heading("title", text="Title")
tv_books.heading("year", text="Year"); tv_books.heading("author", text="Author")
tv_books.column("id", width=50, anchor="center"); tv_books.column("title", width=420)
tv_books.column("year", width=70, anchor="center"); tv_books.column("author", width=260)
tv_books.pack(fill="both", expand=True, padx=6, pady=6)

r_authors(); r_books()
root.mainloop()
