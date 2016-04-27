from lib.FileManager.workers.main.MainWorker import MainWorkerCustomer
import traceback
from config.main import DB_FILE
import sqlite3


class SaveSettings(MainWorkerCustomer):
    def __init__(self, params, *args, **kwargs):
        super(SaveSettings, self).__init__(*args, **kwargs)

        self.viewer_settings = params.get('viewer_settings')
        self.editor_settings = params.get('editor_settings')

    def run(self):
        try:
            self.preload()

            self.update_viewer_settings()
            self.update_editor_settings()

            result = {
                "data": {
                    "editor_settings": self.editor_settings,
                    "viewer_settings": self.viewer_settings
                },
                "error": False,
                "message": None,
                "traceback": None
            }

            self.on_success(result)

        except Exception as e:
            result = {
                "error": True,
                "message": str(e),
                "traceback": traceback.format_exc()
            }

            self.on_error(result)

    def update_viewer_settings(self):
        db = sqlite3.connect(DB_FILE)
        db.execute("PRAGMA journal_mode=MEMORY")
        print("Database created and opened successfully file = %s" % DB_FILE)

        cursor = db.cursor()

        try:
            cursor.execute('''UPDATE viewer_settings SET
                                code_folding_type = ?,
                                font_size = ?,
                                full_line_selection = ?,
                                highlight_active_line = ?,
                                highlight_selected_word = ?,
                                print_margin_size = ?,
                                show_invisible = ?,
                                show_line_numbers = ?,
                                show_print_margin = ?,
                                tab_size = ?,
                                theme = ?,
                                use_soft_tabs = ?,
                                wrap_lines = ?
                              WHERE fm_login = ?
                           ''', (
                self.viewer_settings.get("code_folding_type"),
                self.viewer_settings.get("font_size"),
                self.viewer_settings.get("full_line_selection"),
                self.viewer_settings.get("highlight_active_line"),
                self.viewer_settings.get("highlight_selected_word"),
                self.viewer_settings.get("print_margin_size"),
                self.viewer_settings.get("show_invisible"),
                self.viewer_settings.get("show_line_numbers"),
                self.viewer_settings.get("show_print_margin"),
                self.viewer_settings.get("tab_size"),
                self.viewer_settings.get("theme"),
                self.viewer_settings.get("use_soft_tabs"),
                self.viewer_settings.get("wrap_lines"),
                self.login
            ))

            db.commit()
            if cursor.rowcount < 1:
                raise Exception("viewer_settings update failed")

        except Exception as e:
            raise e
        finally:
            db.close()

    def update_editor_settings(self):
        db = sqlite3.connect(DB_FILE)
        db.execute("PRAGMA journal_mode=MEMORY")
        print("Database created and opened successfully file = %s" % DB_FILE)

        cursor = db.cursor()

        try:
            cursor.execute('''UPDATE editor_settings SET
                                code_folding_type = ?,
                                enable_emmet = ?,
                                font_size = ?,
                                full_line_selection = ?,
                                highlight_active_line = ?,
                                highlight_selected_word = ?,
                                print_margin_size = ?,
                                show_invisible = ?,
                                show_line_numbers = ?,
                                show_print_margin = ?,
                                tab_size = ?,
                                theme = ?,
                                use_autocompletion = ?,
                                use_soft_tabs = ?,
                                wrap_lines = ?
                              WHERE fm_login = ?
                           ''', (
                self.editor_settings.get("code_folding_type"),
                self.editor_settings.get("enable_emmet"),
                self.editor_settings.get("font_size"),
                self.editor_settings.get("full_line_selection"),
                self.editor_settings.get("highlight_active_line"),
                self.editor_settings.get("highlight_selected_word"),
                self.editor_settings.get("print_margin_size"),
                self.editor_settings.get("show_invisible"),
                self.editor_settings.get("show_line_numbers"),
                self.editor_settings.get("show_print_margin"),
                self.editor_settings.get("tab_size"),
                self.editor_settings.get("theme"),
                self.editor_settings.get("use_autocompletion"),
                self.editor_settings.get("use_soft_tabs"),
                self.editor_settings.get("wrap_lines"),
                self.login
            ))

            db.commit()
            if cursor.rowcount < 1:
                raise Exception("editor_settings update failed")

        except Exception as e:
            raise e
        finally:
            db.close()
