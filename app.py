# app.py
import os
import sys
import threading
from tkinter import messagebox
import customtkinter as ctk
from dotenv import load_dotenv
from pipedrive_integration import get_deal, get_pipelines, get_stages
from google_integration import (
    append_deal_to_sheet,
    read_unuploaded_rows,
    mark_uploaded,
    init_google_service,
    find_deal_row_by_id,
    update_deal_row_in_sheet,
    is_row_uploaded
)
from mysql_integration import insert_csv_to_mysql
from datetime import datetime
from zoneinfo import ZoneInfo


# ---------- CONFIG ----------
ENV_FILE = os.path.join("config", ".env")
if not os.path.exists(ENV_FILE):
    messagebox.showerror(
        "Missing config",
        "config/.env not found. Please place the .env inside the config folder."
    )
    sys.exit(1)

load_dotenv(ENV_FILE)
API_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_DB_TABLE = os.getenv("MYSQL_DB_TABLE")


if not API_TOKEN:
    messagebox.showerror("Invalid config", "PIPEDRIVE_API_TOKEN not found in config/.env")
    sys.exit(1)

# ---------- UI ----------
class LastLookApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("LastLook: PreMerge")
        self.geometry("400x700")
        self.resizable(False, False)
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")
        self.configure(fg_color="#fff6de")

        # Main frame
        self.main_frame = ctk.CTkFrame(self, fg_color="#fff6de", corner_radius=12)
        self.main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        # Title
        self.title_label = ctk.CTkLabel(
            self.main_frame,
            text="LastLook: PreMerge",
            font=ctk.CTkFont(size=20, weight="bold"),
            text_color="#273946"
        )
        self.title_label.pack(pady=(12,6))

        # ---------- Styling constants ----------
        PANEL_BG = "#273946"
        APP_BG = "#fff6de"
        ACCENT = "#CB1F47"
        ACCENT_HOVER = "#ffab4c"

        # ---------- Section 1: Take a Snapshot (locked tab header) ----------
        snapshot_tab = self._create_locked_tab_section(
            title="S n a p s h o t",
            height=190,
            panel_bg=PANEL_BG,
            app_bg=APP_BG,
            tab_color=PANEL_BG,
            text_color=ACCENT_HOVER
        )
        self._setup_pull_tab(parent=snapshot_tab)

        # ---------- Section 2: Import to MySQL (locked tab header) ----------
        import_tab = self._create_locked_tab_section(
            title="I m p o r t",
            height=120,
            panel_bg=PANEL_BG,
            app_bg=APP_BG,
            tab_color=PANEL_BG,
            text_color=ACCENT_HOVER
        )
        self._setup_import_tab(parent=import_tab)

        # Progress bar
        self.progress = ctk.CTkProgressBar(
            self.main_frame,
            width=360,
            fg_color="#273946",
            progress_color="#CB1F47"
        )
        self.progress.set(0)
        self.progress.pack(pady=10)

        # Activity Log
        ctk.CTkLabel(
            self.main_frame,
            text="Activity Log",
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color="#273946"
        ).pack(anchor="w", padx=20, pady=(0, 4))

        self.log_box = ctk.CTkTextbox(
            self.main_frame,
            width=360,
            height=200,
            fg_color="#ffffff",
            text_color="#273946"
        )
        self.log_box.pack(padx=10, pady=(0, 10))
        self.log_box.configure(state="disabled")

        self._log("Waiting for action...")

    def _create_locked_tab_section(
        self,
        title: str,
        height: int,
        panel_bg: str,
        app_bg: str,
        tab_color: str,
        text_color: str = "white"
    ):
        """
        Creates a CTkTabview with a single tab, styled like a "tab header",
        then disables interaction so it's purely visual (not clickable).
        Returns the tab frame you can pack widgets into.
        """
        tab_font = ctk.CTkFont(size=12, weight="bold")
        tv = ctk.CTkTabview(self.main_frame, width=360, height=height)
        
        tv.configure(
            fg_color=panel_bg,                    # inside content bg
            segmented_button_fg_color=app_bg,      # background behind the tab button(s)
            segmented_button_selected_color=tab_color,
            segmented_button_selected_hover_color=tab_color,
            segmented_button_unselected_color=tab_color,   # irrelevant w/ 1 tab, but keep consistent
            text_color=text_color,
            text_color_disabled=text_color
        )
        tv.pack(fill="x", padx=10, pady=(10, 8), anchor="w")
        tv.configure(anchor="w")


        tab = tv.add(title)
        TAB_W = 140
        TAB_H = 35

        # ---- Lock it (disable clicking) ----
        # CustomTkinter uses a CTkSegmentedButton internally.
        # Depending on version, we may need to disable the whole segmented button or its child buttons.

        try:
            # left align (optional but helps)
            tv._segmented_button.grid_configure(sticky="w")
        except Exception:
            pass

        try:
            btn = tv._segmented_button._buttons_dict[title]
            btn.configure(width=TAB_W, height=TAB_H)
        except Exception:
            pass
        try:
            tv._segmented_button.configure(state="disabled", font=tab_font)
        except Exception:
            pass

        # Extra safety: disable each internal button if available
        try:
            for btn in tv._segmented_button._buttons_dict.values():
                btn.configure(state="disabled")
        except Exception:
            pass

        return tab

    # ---------- Tabs Setup ----------
    def _setup_pull_tab(self, parent):
        tab = parent

        LABEL_WIDTH = 140

        # Row 1: Deleted Deal ID
        row1 = ctk.CTkFrame(tab, fg_color="transparent")
        row1.pack(fill="x", padx=10, pady=(6, 6), anchor="w")

        ctk.CTkLabel(
            row1,
            text="Deal ID to be deleted:",
            width=LABEL_WIDTH,
            anchor="w",
            text_color="#fff6de"
        ).pack(side="left")

        self.deleted_deal_entry = ctk.CTkEntry(row1, width=220)
        self.deleted_deal_entry.pack(side="left")

        # Row 2: Retained Deal ID
        row2 = ctk.CTkFrame(tab, fg_color="transparent")
        row2.pack(fill="x", padx=10, pady=(6, 10), anchor="w")

        ctk.CTkLabel(
            row2,
            text="Deal ID to be retained:",
            width=LABEL_WIDTH,
            anchor="w",
            text_color="#fff6de"
        ).pack(side="left")

        self.retained_deals_entry = ctk.CTkEntry(row2, width=220)
        self.retained_deals_entry.pack(side="left")

        # Button
        self.pull_btn = ctk.CTkButton(
            tab,
            text="Capture Deal Snapshot",
            fg_color="#CB1F47",
            hover_color="#ffab4c",
            command=self.start_pull_process
        )
        self.pull_btn.pack(pady=(15, 20), padx=10)

    def _setup_import_tab(self, parent):
        tab = parent
        self.import_btn = ctk.CTkButton(
            tab,
            text="Sync Google Sheet → MySQL",
            fg_color="#CB1F47",
            hover_color="#ffab4c",
            command=self.start_import_process
        )
        self.import_btn.pack(pady=(15, 20), padx=10)

    def _show_deal_not_found(self, missing):
        """
        missing: list of tuples -> [("DELETE", id), ("RETAIN", id)]
        Always runs messagebox on the UI thread.
        """
        if len(missing) == 2:
            msg = (
                "Both Deal IDs were not found in Pipedrive:\n\n"
                f"- DELETE Deal ID: {missing[0][1]}\n"
                f"- RETAIN Deal ID: {missing[1][1]}"
            )
        else:
            label, deal_id = missing[0]
            msg = f"{label} Deal ID {deal_id} was not found in Pipedrive."

        def _prompt():
            messagebox.showerror("Deal Not Found", msg)

        self.after(0, _prompt)

    def ask_user_to_update_existing(self, deal_id, row_number):
        result = {"ok": False}
        done = threading.Event()

        msg = (
            f"Deal ID {deal_id} already exists in Google Sheet (row {row_number}).\n\n"
            "Do you want to UPDATE the existing snapshot row?"
        )

        def _prompt():
            result["ok"] = messagebox.askyesno("Snapshot Already Exists", msg)
            done.set()

        self.after(0, _prompt)
        done.wait()
        return result["ok"]

    def ask_user_to_confirm(self, deleted_id, deleted_title, retained_id, retained_title):
        """
        Shows a Yes/No dialog on the UI thread and blocks the worker thread until user answers.
        Returns True if user clicks Yes, else False.
        """
        result = {"ok": False}
        done = threading.Event()

        msg = (
            "Please verify the deals before adding:\n\n"
            f"DELETE Deal ID: {deleted_id}\n"
            f"DELETE Deal Title: {deleted_title}\n\n"
            f"RETAIN Deal ID: {retained_id}\n"
            f"RETAIN Deal Title: {retained_title}\n\n"
            "Proceed and add snapshot to Google Sheet?"
        )

        def _prompt():
            result["ok"] = messagebox.askyesno("Confirm Add", msg)
            done.set()

        # run messagebox in UI thread
        self.after(0, _prompt)

        # wait for user response
        done.wait()
        return result["ok"]

    def _ui_info(self, title, msg):
        self.after(0, lambda: messagebox.showinfo(title, msg))

    def _ui_error(self, title, msg):
        self.after(0, lambda: messagebox.showerror(title, msg))

    def _ui_warn(self, title, msg):
        self.after(0, lambda: messagebox.showwarning(title, msg))

    def _log(self, text: str):
        # Safe to call from any thread
        def _append():
            self.log_box.configure(state="normal")
            self.log_box.insert("end", text + "\n")
            self.log_box.see("end")
            self.log_box.configure(state="disabled")
        self.after(0, _append)

    def _clear_log(self):
        def _do():
            self.log_box.configure(state="normal")
            self.log_box.delete("1.0", "end")
            self.log_box.configure(state="disabled")
        self.after(0, _do)
    
    def _log_divider(self):
        def _append():
            self.log_box.configure(state="normal")
            self.log_box.insert("end", "- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n")
            self.log_box.see("end")
            self.log_box.configure(state="disabled")
        self.after(0, _append)

    # ---------- Progress ----------
    def progress_callback(self, fraction, message=None):
        self.progress.set(fraction)
        if message:
            self._log(message)
        self.update_idletasks()

    # ---------- Take a Snapshot ----------
    def start_pull_process(self):
        self._log_divider()
        deleted_id = self.deleted_deal_entry.get().strip()
        retained_id = self.retained_deals_entry.get().strip()
        # timestamp at click time (Houston / Central Time)
        snapshot_dt = datetime.now(ZoneInfo("America/Chicago"))
        snapshot_str = snapshot_dt.strftime("%Y-%m-%d %H:%M:%S")  # or use ISO if you prefer


        if not deleted_id:
            messagebox.showwarning("Missing Input", "Please enter the Deal ID to be deleted.")
            return

        if not retained_id:
            messagebox.showwarning("Missing Input", "Please enter the Deal ID to be retained after merging.")
            return

        # enforce numeric + single id (no commas/spaces)
        if "," in retained_id:
            messagebox.showwarning("Invalid Input", "Please enter only ONE retained Deal ID (no commas).")
            return

        # validate deleted deal id
        if not deleted_id.isdigit():
            messagebox.showerror(
                "Invalid Deal ID",
                "Deal ID to be deleted is invalid.\n\nIt should be numeric."
            )
            return

        # validate retained deal id
        if not retained_id.isdigit():
            messagebox.showerror(
                "Invalid Deal ID",
                "Deal ID to be retained is invalid.\n\nIt should be numeric."
            )
            return


        self.pull_btn.configure(state="disabled")
        threading.Thread(
            target=self._pull_worker,
            args=(deleted_id, retained_id, snapshot_str),
            daemon=True
        ).start()

    def _pull_worker(self, deleted_id, retained_id, snapshot_str):
        try:
            self.progress_callback(0, "Fetching deals from Pipedrive...")

            missing = []

            def safe_get_deal(deal_id_int, label):
                try:
                    d = get_deal(deal_id_int, API_TOKEN)
                    if not d:
                        missing.append((label, str(deal_id_int)))
                    return d
                except Exception as e:
                    # if your get_deal raises on 404/not found, treat as missing
                    msg = str(e).lower()
                    if "404" in msg or "not found" in msg:
                        missing.append((label, str(deal_id_int)))
                        return None
                    raise  # other errors should still bubble up

            deleted_deal = safe_get_deal(int(deleted_id), "DELETE")
            retained_deal = safe_get_deal(int(retained_id), "RETAIN")

            if missing:
                self._show_deal_not_found(missing)
                self.progress_callback(0, "Waiting for action...")
                return

            deleted_title = deleted_deal.get("Deal - Title", "")
            retained_title = retained_deal.get("Deal - Title", "")

            ok = self.ask_user_to_confirm(
                deleted_id, deleted_title,
                retained_id, retained_title
            )
            if not ok:
                self.progress_callback(0, "Cancelled by user.")
                return

            deleted_deal["Merged with Deal ID"] = int(retained_id)
            deleted_deal["Snapshot Date"] = snapshot_str

            self.progress_callback(0.5, "Checking Google Sheet for existing snapshot...")
            service = init_google_service(GOOGLE_CREDS_JSON)

            existing_row = find_deal_row_by_id(service, GOOGLE_SHEET_ID, deleted_id)

            if existing_row:
                if is_row_uploaded(service, GOOGLE_SHEET_ID, existing_row):
                    self._ui_warn(
                        "Update Blocked",
                        f"Deal ID {deleted_id} already exists in Google Sheet (row {existing_row}) "
                        "and is already uploaded in the CM database.\n\n"
                        "Updates are not allowed after upload.\n\n"
                        "Contact the system developer if a correction is required."
                    )
                    self.progress_callback(0, "Waiting for action...")
                    return
                
                ok_update = self.ask_user_to_update_existing(deleted_id, existing_row)
                if not ok_update:
                    self.progress_callback(0, "Cancelled by user.")
                    return

                self.progress_callback(0.7, f"Updating existing snapshot (row {existing_row})...")
                update_deal_row_in_sheet(service, deleted_deal, GOOGLE_SHEET_ID, existing_row)
            else:
                self.progress_callback(0.7, "Adding new snapshot row...")
                append_deal_to_sheet(service, deleted_deal, GOOGLE_SHEET_ID)


            self.progress_callback(1.0, f"Deal {deleted_id} added successfully!")
            self._ui_info("Success", f"Deal {deleted_id} added to Google Sheet.")

        except Exception as e:
            self._ui_error("Error", str(e))

        finally:
            self.after(0, lambda: self.pull_btn.configure(state="normal"))


    # ---------- Import to MySQL ----------
    def start_import_process(self):
        self._log_divider()
        if not all([MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, MYSQL_DB_TABLE]):
            messagebox.showwarning("MySQL credentials missing", "Please set MySQL credentials in .env to proceed.")
            return

        self.import_btn.configure(state="disabled")
        threading.Thread(target=self._import_worker, daemon=True).start()

    def _import_worker(self):
        try:
            self.progress_callback(0, "Reading unuploaded rows from Google Sheet...")
            rows = read_unuploaded_rows(
                token_path="token.json",
                sheet_id=GOOGLE_SHEET_ID,
                worksheet_name="Sheet1"
            )


            if not rows:
                self._ui_info("Nothing to Import", "No new deals found in Google Sheet.")
                self.progress_callback(0, "Waiting for action...")
                return

            self.progress_callback(0.3, "Generating CSV and inserting into MySQL...")
            csv_path = "temp_lastlook_import.csv"
            import pandas as pd
            pd.DataFrame(rows).to_csv(csv_path, index=False)

            insert_csv_to_mysql(
                csv_path,
                {
                    "host": MYSQL_HOST,
                    "user": MYSQL_USER,
                    "password": MYSQL_PASSWORD,
                    "db": MYSQL_DB,
                },
                table_name=MYSQL_DB_TABLE
            )

            deal_ids = [r["Deal - ID"] for r in rows]
            mark_uploaded(
                token_path="token.json",
                sheet_id=GOOGLE_SHEET_ID,
                deal_ids=deal_ids,
                worksheet_name="Sheet1"
            )

            self.progress_callback(1.0, "Import completed successfully!")
            self._ui_info("Success", f"{len(rows)} deals imported to MySQL and marked as uploaded.")
        except Exception as e:
            self._ui_error("Error", str(e))
        finally:
            self.after(0, lambda: self.import_btn.configure(state="normal"))
            self.progress_callback(0, "Waiting for action...")

if __name__ == "__main__":
    app = LastLookApp()
    app.mainloop()
