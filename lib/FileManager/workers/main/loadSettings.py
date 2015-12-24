from lib.FileManager.workers.baseWorkerCustomer import BaseWorkerCustomer
import traceback
from config.main import DB_FILE
import sqlite3


class LoadSettings(BaseWorkerCustomer):
    def __init__(self, *args, **kwargs):
        super(LoadSettings, self).__init__(*args, **kwargs)

    def run(self):
        try:
            self.preload()
            result = {
                "data": {
                    "editor_settings": self.get_editor_settings(),
                    "viewer_settings": self.get_viewer_settings()
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

    def get_viewer_settings(self):
        db = sqlite3.connect(DB_FILE)
        db.execute("PRAGMA journal_mode=MEMORY")
        print("Database created and opened successfully file = %s" % DB_FILE)

        cursor = db.cursor()

        try:
            cursor.execute("SELECT "
                           "fm_login, "
                           "code_folding_type, "
                           "font_size, "
                           "full_line_selection, "
                           "highlight_active_line, "
                           "highlight_selected_word, "
                           "print_margin_size, "
                           "show_invisible, "
                           "show_line_numbers, "
                           "show_print_margin, "
                           "tab_size, "
                           "theme, "
                           "use_soft_tabs, "
                           "wrap_lines "
                           "FROM viewer_settings WHERE fm_login = ?", (self.login,))
            result = cursor.fetchone()

            if result is None:
                settings = {
                    "login": self.login,
                    "code_folding_type": "manual",
                    "font_size": 14,
                    "full_line_selection": True,
                    "highlight_active_line": True,
                    "highlight_selected_word": True,
                    "print_margin_size": 85,
                    "show_invisible": False,
                    "show_line_numbers": True,
                    "show_print_margin": False,
                    "tab_size": 4,
                    "theme": "clouds",
                    "use_soft_tabs": True,
                    "wrap_lines": False,
                }

                cursor.execute(
                        "INSERT INTO viewer_settings ("
                        "fm_login, "
                        "code_folding_type, "
                        "font_size, "
                        "full_line_selection, "
                        "highlight_active_line, "
                        "highlight_selected_word, "
                        "print_margin_size, "
                        "show_invisible, "
                        "show_line_numbers, "
                        "show_print_margin, "
                        "tab_size, "
                        "theme, "
                        "use_soft_tabs, "
                        "wrap_lines"
                        ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            settings.get("login"),
                            settings.get("code_folding_type"),
                            settings.get("font_size"),
                            settings.get("full_line_selection"),
                            settings.get("highlight_active_line"),
                            settings.get("highlight_selected_word"),
                            settings.get("print_margin_size"),
                            settings.get("show_invisible"),
                            settings.get("show_line_numbers"),
                            settings.get("show_print_margin"),
                            settings.get("tab_size"),
                            settings.get("theme"),
                            settings.get("use_soft_tabs"),
                            settings.get("wrap_lines"),
                        ))

                db.commit()
            else:
                settings = {
                    "login": result[0],
                    "code_folding_type": result[1],
                    "font_size": result[2],
                    "full_line_selection": result[3],
                    "highlight_active_line": result[4],
                    "highlight_selected_word": result[5],
                    "print_margin_size": result[6],
                    "show_invisible": result[7],
                    "show_line_numbers": result[8],
                    "show_print_margin": result[9],
                    "tab_size": result[10],
                    "theme": result[11],
                    "use_soft_tabs": result[12],
                    "wrap_lines": result[13]
                }

            return settings
        except Exception as e:
            raise e
        finally:
            db.close()

    def get_editor_settings(self):

        db = sqlite3.connect(DB_FILE)
        db.execute("PRAGMA journal_mode=MEMORY")
        print("Database created and opened successfully file = %s" % DB_FILE)

        cursor = db.cursor()

        try:
            cursor.execute("SELECT "
                           "fm_login, "
                           "code_folding_type, "
                           "enable_emmet, "
                           "font_size, "
                           "full_line_selection, "
                           "highlight_active_line, "
                           "highlight_selected_word, "
                           "print_margin_size, "
                           "show_invisible, "
                           "show_line_numbers, "
                           "show_print_margin, "
                           "tab_size, "
                           "theme, "
                           "use_autocompletion, "
                           "use_soft_tabs, "
                           "wrap_lines "
                           "FROM editor_settings WHERE fm_login = ?", (self.login,))
            result = cursor.fetchone()

            if result is None:
                settings = {
                    "login": self.login,
                    "code_folding_type": "manual",
                    "enable_emmet": True,
                    "font_size": 14,
                    "full_line_selection": True,
                    "highlight_active_line": True,
                    "highlight_selected_word": True,
                    "print_margin_size": 85,
                    "show_invisible": False,
                    "show_line_numbers": True,
                    "show_print_margin": False,
                    "tab_size": 4,
                    "theme": "clouds",
                    "use_autocompletion": True,
                    "use_soft_tabs": True,
                    "wrap_lines": False,
                }

                cursor.execute(
                        "INSERT INTO editor_settings ("
                        "fm_login, "
                        "code_folding_type, "
                        "enable_emmet, "
                        "font_size, "
                        "full_line_selection, "
                        "highlight_active_line, "
                        "highlight_selected_word, "
                        "print_margin_size, "
                        "show_invisible, "
                        "show_line_numbers, "
                        "show_print_margin, "
                        "tab_size, "
                        "theme, "
                        "use_autocompletion, "
                        "use_soft_tabs, "
                        "wrap_lines "
                        ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            settings.get("login"),
                            settings.get("code_folding_type"),
                            settings.get("enable_emmet"),
                            settings.get("font_size"),
                            settings.get("full_line_selection"),
                            settings.get("highlight_active_line"),
                            settings.get("highlight_selected_word"),
                            settings.get("print_margin_size"),
                            settings.get("show_invisible"),
                            settings.get("show_line_numbers"),
                            settings.get("show_print_margin"),
                            settings.get("tab_size"),
                            settings.get("theme"),
                            settings.get("use_autocompletion"),
                            settings.get("use_soft_tabs"),
                            settings.get("wrap_lines"),
                        ))

                db.commit()
            else:
                settings = {
                    "login": result[0],
                    "code_folding_type": result[1],
                    "enable_emmet": result[2],
                    "font_size": result[3],
                    "full_line_selection": result[4],
                    "highlight_active_line": result[5],
                    "highlight_selected_word": result[6],
                    "print_margin_size": result[7],
                    "show_invisible": result[8],
                    "show_line_numbers": result[9],
                    "show_print_margin": result[10],
                    "tab_size": result[11],
                    "theme": result[12],
                    "use_autocompletion": result[13],
                    "use_soft_tabs": result[14],
                    "wrap_lines": result[15]
                }

            return settings
        except Exception as e:
            raise e
        finally:
            db.close()
