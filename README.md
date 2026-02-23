# LastLook: PreMerge

**LastLook: PreMerge** is a desktop utility (built with Python) that implements a
**controlled Extract–Transform–Load (ETL) snapshot workflow** for Pipedrive deal merges.

It captures a **pre-merge snapshot** of a Pipedrive deal, stages that snapshot in Google Sheets for validation and audit visibility, and then persists approved records into MySQL for long-term retention.

---

![Version](https://img.shields.io/badge/version-1.0.0-ffab4c?style=for-the-badge&logo=python&logoColor=white)
![Python](https://img.shields.io/badge/python-3.11%2B-273946?style=for-the-badge&logo=python&logoColor=ffab4c)
![Status](https://img.shields.io/badge/status-active-273946?style=for-the-badge&logo=github&logoColor=ffab4c)

---

## 🚧 Problem Statement / Motivation

When two deals are merged in Pipedrive, the “deleted” or absorbed deal permanently loses
important historical context such as ownership, stage,
activity counts, etc.

This behavior creates a gap against the CM’s mandate for **Total Data Preservation**:
once a merge is completed, there is no native, durable record of the full state of the deleted
deal prior to the merge. Relying on manual copy/paste or screenshots introduces inconsistency,
human error, and an incomplete audit trail.

To align operational workflows with this mandate, a structured and repeatable pre-merge
process is required—one that extracts deal data before irreversible actions occur, transforms
it into a normalized and readable form, and stores it in a cloud-based repository designed for long-term
retention and auditability.

LastLook: PreMerge addresses this gap by enforcing a controlled snapshot workflow:
1. Extract deal data from Pipedrive before merge.
2. Confirm merge intent and deal pairing with the user.
3. Stage a normalized snapshot in Google Sheets for visibility and review.
4. Store snapshots into MySQL as a durable system of record.
5. Lock and track uploaded records to prevent accidental modification or loss.

As part of this process, once the PD team identifies which deals will be deleted or retained, a pre-merge snapshot of the deal to be deleted must be captured using this tool before the actual merge is executed in Pipedrive.

---

## ✨ Features
- **Deal snapshot capture from Pipedrive** using deal ID (to be deleted) + retained deal ID.
- **Google Sheets append/update flow**:
  - Prevents duplicate snapshots by checking existing `Deal - ID`.
  - Blocks updates if row is already marked `Uploaded = YES`.
- **MySQL sync flow**:
  - Reads only rows not marked uploaded.
  - Maps Google Sheet headers to DB schema.
  - Performs datatype conversion (ints, decimals, datetimes).
  - Marks imported rows as uploaded in Google Sheets.
- **Operator-friendly desktop UI** with:
  - Validation for numeric IDs.
  - Progress bar.
  - Activity log.
  - Confirmation prompts and clear error dialogs.

---

## 🧠 Logic Flow
### Snapshot flow
1. User enters:
   - Deal ID to be deleted.
   - Deal ID to be retained.
2. App validates input:
   - Deal IDs must be numeric
   - Deal IDs must exist in Pipedrive
3. App fetches both deals from Pipedrive.
4. User confirms the two deal titles/IDs for initial validation.
5. App enriches the record of the deal to be deleted in the Google Sheet with the following fields:
   - `Merged with Deal ID` (Deal ID to be retained)
   - `Snapshot Date` (current timestamp in the America/Chicago timezone)
6. App checks Google Sheet:
   - If deal already exists and uploaded -> block update.
   - If deal exists and not uploaded -> ask to update row.
   - Else -> append a new row.

### Import flow
1. App reads all rows from `Sheet1` where `Uploaded != YES`.
2. App creates an audit copy in `xlsx` format.
3. App maps sheet columns to MySQL table columns.
4. App inserts rows into the configured MySQL table.
5. App marks those deal IDs as `Uploaded = YES` in Google Sheets.

---

## 📝 Requirements
- **Python**: 3.10+ recommended.
- **APIs / Services**:
  - Pipedrive API token.
  - Google Cloud OAuth client credentials JSON (Sheets scope).
  - Google Sheet with header row configured.
  - MySQL database + existing destination table.
- **Python packages** (high-level):
  - UI: `customtkinter`
  - HTTP/API: `requests`
  - Google integration: `google-api-python-client`, `google-auth`, `google-auth-oauthlib`
  - Data handling: `pandas`
  - DB connector: `mysql-connector-python`
  - Config: `python-dotenv`

> Install all pinned dependencies from `requirements.txt`.

## 🚀 Installation and Setup
1. **Clone repository**
    ```bash
    git clone https://github.com/dyoliya/last_look_premerge.git
    cd last_look_premerge
    ```

2. **Install dependencies**
    ```bash
    pip install -r requirements.txt
    ```

3. **Folder Structure**

    <pre>project/
    │
    ├── app.py                  Main application entry point
    ├── audit/                  Folder containing generated XLSX audit files
    ├── google_integration.py   Google Sheets integration logic
    ├── mysql_integration.py    MySQL database integration logic
    ├── pipedrive_api.py        Pipedrive API wrapper
    ├── config/                 Configuration files
    │   ├── .env                Environment variables
    │   └── google_creds.json   Google OAuth credentials
    └── token.json              Cached Google OAuth token
    </pre>

    
    
4. **Set Up Configuration**

    Before running the tool, you need to provide the app with your Pipedrive API token so it can access deal data.

    4.1. **Pipedrive API Token**

   - Log in to your Pipedrive account.
   - Click your Profile → Personal preferences.
   - Under the Account section, click the API tab. You will see Your Personal Token.
   - Copy your API token.
   - Open the `config/.env` file (create it if it doesn’t exist).
   - Add the following line, replacing <YOUR_API_TOKEN> with your actual token:
     ```env
     PIPEDRIVE_API_TOKEN=<YOUR_API_TOKEN>
     ```
   - Save the file. The app will use this token to authenticate requests to Pipedrive.

    4.2 Google Credentials (Preconfigured)
    
   - Google OAuth credentials are provided by the technical team and do not require setup by end users.
   - The credentials file will already be included in the app folder.
   - The .env file will reference the correct path.
   - No manual changes are required unless instructed by the tool developer/admin.

    4.3 MySQL Credentials (Restricted Access)
    
   - MySQL access is only required for users authorized to import data into the database.
   - Not all users will receive MySQL credentials.
   - Users without MySQL access can still:
     - Capture deal snapshots
     - Write snapshots to Google Sheets
    
      If you are authorized for MySQL import, add the following lines to your `.env` file and replace the placeholder values with your actual credentials:
      ```env
      MYSQL_HOST=localhost
      MYSQL_USER=your_mysql_user
      MYSQL_PASSWORD=your_mysql_password
      MYSQL_DB=your_database_name
      MYSQL_DB_TABLE=your_table_name

6. **Prepare Google Sheet**
- Ensure worksheet name is `Sheet1`.
- Ensure row 1 has headers matching expected fields (including `Deal - ID`; optionally `Uploaded`).

7. **Compile the tool**
   ```bash
   pyinstaller --onefile --windowed --name last_look_premerge.py

On first Google-auth use, a browser window may open for OAuth consent; a local `token.json` is stored for future runs.

## 🖥️ User Guide

### A. First-time preparation
1. Open the app folder.
2. Confirm there is a `config` folder.
3. Inside `config`, confirm `.env` exists and was prefilled by your admin.
4. Confirm Google credentials file exists at the path your admin provided.

### B. Capture a snapshot before merge
1. Launch the app executable.
2. In **Snapshot** section:
   - Enter **Deal ID to be deleted**.
   - Enter **Deal ID to be retained**.
3. Click **Capture Deal Snapshot**.
4. When confirmation pop-up appears, verify titles and IDs.
5. Click **Yes** to proceed.
6. Based on the existing record status in Google Sheets, the app will do one of the following:
    * Append a new row if the deal ID does not yet exist.
    * Update the existing row if the deal ID exists but is not yet uploaded to MySQL.
    * Block the action if the deal ID already exists and is marked as uploaded.
7. Wait for success message in the Activity Log section.

### C. Sync snapshots into database
1. Go to **Import** section.
2. Click **Sync Google Sheet → MySQL**.
3. Wait for completion message.
4. The app will mark imported rows as `Uploaded = YES` in Google Sheets.

### D. Important usage notes
- Only one retained deal ID is allowed per snapshot.
- IDs must be numeric.
- If a deal snapshot already exists and is already uploaded, edits are blocked.
- If no unuploaded rows exist, import safely does nothing.
- Row deletion is restricted to protect historical accuracy. Once a deal is merged in Pipedrive, the original state of that deal can no longer be recreated/recaptured by the tool. Removing a snapshot would permanently remove the only preserved record of that pre-merge state. Hence, if any deletion or removal of entries in the Google Sheet and MySQL table is required, please contact the tool developer for assistance.

### E. Common troubleshooting
- **“config/.env not found”**: Place `.env` inside the `config` folder.
- **Pipedrive auth errors**: Verify your correct API token in `.env`.
- **Google access errors**: Verify credentials file path and that sheet is shared correctly.
- **MySQL errors**: Verify DB host/user/password/table and that schema matches expected mapped columns.

---

## 👩‍💻 Credits
- **2026-02-04**: Project created by **Julia** ([@dyoliya](https://github.com/dyoliya))  
- 2026–present: Maintained by **Julia** for **Community Minerals II, LLC**
