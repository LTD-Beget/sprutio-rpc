#!/usr/bin/env python

import sqlite3
import os
import traceback

from config.main import DB_FILE

if os.path.exists(DB_FILE):
    print("Database already created")
else:
    db = sqlite3.connect(DB_FILE)
    db.execute("PRAGMA journal_mode=MEMORY")
    print("Database created and opened successfully file = %s" % DB_FILE)

    cursor = db.cursor()

    try:
        cursor.execute('''CREATE TABLE ftp_servers (
                            id          INTEGER PRIMARY KEY NOT NULL,
                            fm_login    TEXT,
                            host        TEXT,
                            port        INTEGER,
                            user        TEXT,
                            password    TEXT
                          );''')

        cursor.execute('''CREATE INDEX ftp_servers_fm_login ON ftp_servers (fm_login);''')

        cursor.execute('''CREATE TABLE editor_settings (
                            id                        INTEGER PRIMARY KEY NOT NULL,
                            fm_login                  TEXT,
                            print_margin_size         INTEGER,
                            font_size                 INTEGER,
                            tab_size                  INTEGER,
                            full_line_selection       INTEGER,
                            highlight_active_line     INTEGER,
                            show_invisible            INTEGER,
                            wrap_lines                INTEGER,
                            use_soft_tabs             INTEGER,
                            show_line_numbers         INTEGER,
                            highlight_selected_word   INTEGER,
                            show_print_margin         INTEGER,
                            use_autocompletion        INTEGER,
                            enable_emmet              INTEGER,
                            code_folding_type         TEXT,
                            theme                     TEXT
                          );''')

        cursor.execute('''CREATE INDEX editor_settings_fm_login ON editor_settings (fm_login);''')

        cursor.execute('''CREATE TABLE viewer_settings (
                            id                        INTEGER PRIMARY KEY NOT NULL,
                            fm_login                  TEXT,
                            print_margin_size         INTEGER,
                            font_size                 INTEGER,
                            tab_size                  INTEGER,
                            full_line_selection       INTEGER,
                            highlight_active_line     INTEGER,
                            show_invisible            INTEGER,
                            wrap_lines                INTEGER,
                            use_soft_tabs             INTEGER,
                            show_line_numbers         INTEGER,
                            highlight_selected_word   INTEGER,
                            show_print_margin         INTEGER,
                            code_folding_type         TEXT,
                            theme                     TEXT
                          );''')

        cursor.execute('''CREATE INDEX viewer_settings_fm_login ON viewer_settings (fm_login);''')

        print("All tables created successfully")
        db.commit()
    except Exception as e:
        print('Error while creating DB %s -- Traceback: %s' % (str(e), traceback.format_exc()))
    finally:
        db.close()
