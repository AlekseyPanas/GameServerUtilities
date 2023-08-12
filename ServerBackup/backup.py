"""
Create a google service account and download the credentials as a json file into the same folder as this script.

Make sure you share a google drive folder with this service account where you want your backups stored

Place this script anywhere within your ark server's directory.

Execute the script to perform a backup.

The script will prompt you to create a configuration file if you haven't made one yet. It will also guide
you through the process with prompts

The script will automatically search for the "Saved" folder within your ark server directory. This target folder
contains ALL saves for ALL arks, clusters, configs, and more.
"""
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from oauth2client.service_account import ServiceAccountCredentials
from abc import abstractmethod
import time
from datetime import datetime
import os
from dataclasses import dataclass
import re
from typing import Any, Callable


class ANSI:
    BLACK = "\033[0;30m"
    RED = "\033[0;31m"
    GREEN = "\033[0;32m"
    BROWN = "\033[0;33m"
    BLUE = "\033[0;34m"
    PURPLE = "\033[0;35m"
    CYAN = "\033[0;36m"
    LIGHT_GRAY = "\033[0;37m"
    DARK_GRAY = "\033[1;30m"
    LIGHT_RED = "\033[1;31m"
    LIGHT_GREEN = "\033[1;32m"
    YELLOW = "\033[1;33m"
    LIGHT_BLUE = "\033[1;34m"
    LIGHT_PURPLE = "\033[1;35m"
    LIGHT_CYAN = "\033[1;36m"
    LIGHT_WHITE = "\033[1;37m"
    BOLD = "\033[1m"
    FAINT = "\033[2m"
    ITALIC = "\033[3m"
    UNDERLINE = "\033[4m"
    BLINK = "\033[5m"
    NEGATIVE = "\033[7m"
    CROSSED = "\033[9m"
    END = "\033[0m"

    @staticmethod
    def print_col(msg: str, col: str):
        print(col + msg + ANSI.END)

    @staticmethod
    def get_colored_input(prompt: str, col_prompt: str, col_text: str) -> str:
        o = input(col_prompt + prompt + ANSI.END + col_text).strip()
        print(ANSI.END, end="")
        return o

    @staticmethod
    def get_int(mn: int = None, mx: int = None, prompt_col=PURPLE, error_col=RED, input_color=YELLOW):
        while True:
            try:
                i = int(ANSI.get_colored_input("> ", prompt_col, input_color))
                if mn is not None and i < mn:
                    ANSI.print_col(f"Must be >= {mn}", error_col)
                elif mx is not None and i > mx:
                    ANSI.print_col(f"Must be <= {mx}", error_col)
                else:
                    return i
            except Exception:
                ANSI.print_col("Not an integer, try again", error_col)

    @staticmethod
    def select_from_multiple(options: list[Any], prompt_if_multiple: str, error_msg: str, single_found_message_prefix: str, select_confirmation_message_prefix: str, str_function: Callable[[Any], str] = (lambda x: x), error_col=LIGHT_RED, print_col=DARK_GRAY, prompt_col=PURPLE, input_col=YELLOW) -> Any:
        """
        Given a list of options, checks how many there are. If none, exit with error. If one, return it with a printed message. If multiple, use python input to prompt the user to select one.
        prompt_if_multiple, error_msg, single_found_message_prefix, and select_confirmation_message_prefix all customize the printed messages
        str_function is a function to convert the values in options into strings if necessary. Gives customization to how the options will be listed
        """
        if len(options) == 0:
            ANSI.print_col(error_msg, error_col)
            exit(1)
        elif len(options) == 1:
            option = options[0]
            ANSI.print_col(single_found_message_prefix + f": {str_function(option)}", print_col)
        else:
            ANSI.print_col(prompt_if_multiple + ":\n\t" + "\n\t".join(
                [f"[{i}] {str_function(options[i])}" for i in range(len(options))]), print_col)
            ANSI.print_col("Select one:", prompt_col)
            idx = ANSI.get_int(0, len(options) - 1, prompt_col, error_col, input_col)
            option = options[idx]
            ANSI.print_col(select_confirmation_message_prefix + f": {option}", print_col)
        return option


class CloudInterface:
    @dataclass
    class CloudFile:
        name: str
        parent_ids: list[str]
        created_time: datetime
        mimetype: str
        cloud_id: str

    @abstractmethod
    def create_folder(self, name_on_cloud: str, parent_folder_id: str) -> str:
        """
        Create a folder on the cloud
            - name_on_cloud: Name of the new folder on the cloud
            - parent_folder_id: ID of the folder in which to place this new folder
        return ID of this new folder
        """

    @abstractmethod
    def upload_file(self, path_to_local: str, name_on_cloud: str, parent_folder_id: str) -> str:
        """
        Upload a single local file to the cloud
            - path_to_local: path to the target file being uploaded
            - name_on_cloud: Name of this file you would like on the cloud. If None, use same name as local file
            - parent_folder_id: ID of the folder in which to place this upload
        return ID of this new file
        """

    @abstractmethod
    def upload_folder(self, path_to_local: str, name_on_cloud: str, parent_folder_id: str) -> str:
        """
        Upload an entire local folder to the cloud
            - path_to_local: path to the target folder being uploaded
            - name_on_cloud: Name of this folder you would like on the cloud
            - parent_folder_id: ID of the folder in which to place this upload
        return ID of this new folder
        """

    @abstractmethod
    def delete_cloud_file(self, file_id: str):
        """
        Delete a file on the cloud
            - file_id: ID of this file on the cloud
        """

    @abstractmethod
    def get_all_content(self) -> list[CloudFile]:
        """Return a list of ALL cloud files accessible by this API."""


class GoogleDrive(CloudInterface):
    def __init__(self, cred_json_path: str, silent=False, print_col=ANSI.DARK_GRAY):
        scope = ['https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(cred_json_path, scope)
        self.__service = build('drive', 'v3', credentials=creds)

        self.silent = silent
        self.print_col = print_col

    def create_folder(self, name_on_cloud: str, parent_folder_id: str) -> str:
        return self.__service.files().create(
            body={"name": name_on_cloud, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_folder_id]},
            fields="id").execute()["id"]

    def upload_file(self, path_to_local: str, name_on_cloud: str | None, parent_folder_id: str) -> str:
        body = {"name": name_on_cloud, "parents": [parent_folder_id]} if name_on_cloud is not None else {"parents": [parent_folder_id]}

        media = MediaFileUpload(path_to_local, resumable=True)
        fid = self.__service.files().create(
            body=body,
            media_body=media, fields="id").execute()

        return fid["id"]

    def upload_folder(self, path_to_local: str, name_on_cloud: str, parent_folder_id: str) -> str:
        split_root_path = os.path.split(path_to_local)
        cloud_root_folder_id = self.create_folder(name_on_cloud, parent_folder_id)

        def rec_helper(cur_subpath: tuple, parent_folder_id: str):
            _, folders, files = next(os.walk(os.path.join(*cur_subpath)))

            for fol in folders:
                # Make the folder on the cloud
                fol_id = self.create_folder(fol, parent_folder_id)
                if not self.silent: ANSI.print_col(f"Folder Created: {os.path.join(*(cur_subpath + (fol,)))}", self.print_col)

                # recursively call with the new folder as parent
                rec_helper(cur_subpath + (fol,), fol_id)

            for fil in files:
                # Upload file
                self.upload_file(os.path.join(*(cur_subpath + (fil,))), fil, parent_folder_id)
                if not self.silent: ANSI.print_col(f"File Uploaded: {os.path.join(*(cur_subpath + (fil,)))}", self.print_col)

        rec_helper(split_root_path, cloud_root_folder_id)
        return cloud_root_folder_id

    def delete_cloud_file(self, file_id: str):
        self.__service.files().delete(fileId=file_id).execute()

    def get_all_content(self) -> list[CloudInterface.CloudFile]:
        all_files = []

        page_token = ""

        while True:

            out: dict = self.__service.files().list(fields="nextPageToken, files(id, name, parents, createdTime, mimeType)", pageToken=page_token).execute()
            page_token = out.get("nextPageToken")

            all_files += [CloudInterface.CloudFile(
                o["name"], o.get("parents", []), datetime.strptime(o["createdTime"].split(".")[0], '%Y-%m-%dT%H:%M:%S'),
                o["mimeType"], o["id"]
            ) for o in out["files"]]

            if page_token is None:
                break

        return all_files


if __name__ == "__main__":
    """
    1. Find Saved folder
    2. Recursively generate a list of file paths to ALL files in this folder
    3. Upload the files to drive
    4. Delete the drive save folder with the earliest creation date if there are more than X saved backups
    """
    class ConfigField:
        """
        - name: name of the field in the config file
        """
        def __init__(self, name):
            self.name = name
            self.actual_value: Any | None = None
            self.string_value: Any | None = None

        @abstractmethod
        def parser(self, string_value: str) -> Any:
            """Given the string value from the config file, returns parsed python object"""

        @abstractmethod
        def encoder(self, obj: Any) -> str:
            """Inverse of the parser, takes the python object and returns the config value string"""

        @abstractmethod
        def interactive_getter(self, current_config: dict[str, Any]) -> Any:
            """Shows interactive prompts to acquire a new config value from the user if doesn't exist"""


    class FolderSearchDepthConfig(ConfigField):
        NAME = "SAVED_FOLDER_SEARCH_DEPTH"
        def __init__(self): super().__init__(self.NAME)
        def parser(self, string_value: str) -> Any: return int(re.match("[0-9]+", string_value).group())
        def encoder(self, obj: Any) -> str: return str(obj)
        def interactive_getter(self, current_config: dict[str, Any]) -> Any: ANSI.print_col("[int] Maximum number of folder depth to analyze forward and backward when finding Saved directory: ", ANSI.PURPLE); return ANSI.get_int(mn=1)


    class MaxBackupConfig(ConfigField):
        NAME = "MAX_BACKUP"
        def __init__(self): super().__init__(self.NAME)
        def parser(self, string_value: str) -> Any: return int(re.match("[0-9]+", string_value).group())
        def encoder(self, obj: Any) -> str: return str(obj)
        def interactive_getter(self, current_config: dict[str, Any]) -> Any: ANSI.print_col("[int] Maximum number of backed up directories in google drive. Once exceeded, script will delete oldest: ", ANSI.PURPLE); return ANSI.get_int()

    class SavedFolderConfig(ConfigField):
        NAME = "SAVED_FOLDER_PATH"
        def __init__(self): super().__init__(self.NAME)
        def parser(self, string_value: str) -> Any: return string_value
        def encoder(self, obj: Any) -> str: return str(obj)

        def interactive_getter(self, current_config: dict[str, Any]) -> Any:
            ANSI.print_col("Searching for 'Saved' folder...", ANSI.CYAN)

            def find_saved_folder(start_dir_path: tuple, max_depth: int) -> list[str]:
                """Given start directory, find all folders named 'Saved' and return their paths. Searches downward up to a max_depth"""

                def rec_helper(depth: int, path: tuple, max_depth: int) -> list[str]:
                    # If depth exceeded, return nothing
                    if depth > max_depth:
                        return []
                    else:
                        paths_to_saved = []

                        # Loop through all items in directory of current path
                        for n in os.listdir(os.path.join(*path)):
                            # Parse into single string path
                            path_to_n = os.path.join(*(path + (n,)))
                            # print(path_to_n)

                            # If Saved exists and is a directory, add the path to the list of valid paths
                            if n == "Saved" and os.path.isdir(path_to_n):
                                paths_to_saved.append(path_to_n)
                            # For every folder found, call recursively and add those paths too
                            elif os.path.isdir(path_to_n):
                                paths_to_saved += rec_helper(depth + 1, path + (n,), max_depth)

                        return paths_to_saved

                found_dirs = rec_helper(0, start_dir_path, max_depth)
                # print(found_dirs)
                return found_dirs

            saved_paths = find_saved_folder(("..",) * (current_config[FolderSearchDepthConfig.NAME] + 1), current_config[FolderSearchDepthConfig.NAME] * 2)
            saved_path = ANSI.select_from_multiple(saved_paths,
                                                   "Multiple saved directories found",
                                                   "ERROR: 'Saved' directory not found.\n\tIncrease the dir search depth and make sure the script is in the server directory",
                                                   "'Saved' directory found at",
                                                   "'Saved' directory selected")
            print()
            return saved_path

    class JSONKeyFilePathConfig(ConfigField):
        NAME = "JSON_KEY_FILE_PATH"

        def __init__(self): super().__init__(self.NAME)

        def parser(self, string_value: str) -> Any: return string_value

        def encoder(self, obj: Any) -> str: return str(obj)

        def interactive_getter(self, current_config: dict[str, Any]) -> Any:
            ANSI.print_col("Looking for JSON file...", ANSI.CYAN)

            json_files = [t for t in os.listdir(".") if len(t.split(".")) > 1 and t.split(".")[1] == "json"]
            json_file = ANSI.select_from_multiple(json_files,
                                                  "Multiple JSON files found",
                                                  "ERROR: No JSON file found\n\tPlease make sure you created a google service account and downloaded the JSON private key to this script's directory",
                                                  "JSON file found at",
                                                  "JSON file selected")
            print()
            return json_file

    class ConfigFileParser:
        def __init__(self, config_fields: list[ConfigField], config_file_path: str):
            self.config_fields: dict[str, ConfigField] = {c.name: c for c in config_fields}

            # Generate blank file if doesn't exist
            if not os.path.exists(config_file_path):
                ANSI.print_col("Config not found, creating...", ANSI.CYAN)
                with open(config_file_path, "w"): pass
                ANSI.print_col("Blank config file generated.", ANSI.DARK_GRAY)
                print()

            # Parse lines in conf file and populate config fields where found
            with open(config_file_path, "r") as conf_file:
                lines = [ln[:-1] if ln[-1] == "\n" else ln for ln in conf_file.readlines()]

                for line in lines:
                    toks = line.split("=")

                    if toks[0] in self.config_fields:
                        self.config_fields[toks[0]].string_value = toks[1]
                        try:
                            self.config_fields[toks[0]].actual_value = self.config_fields[toks[0]].parser(toks[1])
                        except Exception:
                            ANSI.print_col(
                                f"ERROR: Config file parsing failed.\n\tValue incorrect for field {toks[0]}",
                                ANSI.LIGHT_RED)
                            exit(1)

            unacquired = [ky for ky in self.config_fields if self.config_fields[ky].actual_value is None]
            with open(config_file_path, "a") as conf_file:
                if len(unacquired) > 0:
                    conf_file.write("\n")
                # Interactively get the fields that weren't acquired
                for k in unacquired:
                    # Get field
                    self.config_fields[k].actual_value = self.config_fields[k].interactive_getter(
                        {ky: self.config_fields[ky].actual_value for ky in self.config_fields}
                    )
                    self.config_fields[k].string_value = self.config_fields[k].encoder(self.config_fields[k].actual_value)

                    # Add to config file
                    conf_file.write(f"{k}={self.config_fields[k].string_value}\n")

            ANSI.print_col("Config file generated:", ANSI.CYAN)
            ANSI.print_col("\n".join(f"{ky}={self.config_fields[ky].string_value}" for ky in self.config_fields), ANSI.DARK_GRAY)
            print()

        def get(self, key: str):
            return self.config_fields[key].actual_value

    ####################################################################################
    # BACKUP STAGE
    ####################################################################################
    CLOUD_FOLDER_PREFIX = "Saved_Backup_"

    configparser = ConfigFileParser([FolderSearchDepthConfig(), MaxBackupConfig(), SavedFolderConfig(), JSONKeyFilePathConfig()], "config")
    cloud: CloudInterface = GoogleDrive(configparser.get(JSONKeyFilePathConfig.NAME))

    files_in_cloud = cloud.get_all_content()

    ANSI.print_col("Looking for root cloud directory...", ANSI.CYAN)
    root_folders = [f for f in files_in_cloud if len(f.parent_ids) == 0]
    root_folder: CloudInterface.CloudFile = ANSI.select_from_multiple(
        root_folders, "Multiple root cloud directories found",
        "ERROR: No root cloud directory found\n\tPlease make sure you shared a folder with your service account or otherwise configured your cloud properly",
        "Root cloud directory found",
        "Root directory selected", str_function=lambda r: f"Name: {r.name}, ID: {r.cloud_id}")
    print()

    ANSI.print_col("Looking for existing backups...", ANSI.CYAN)
    existing_saved = [f for f in files_in_cloud if root_folder.cloud_id in f.parent_ids and f.name.startswith(CLOUD_FOLDER_PREFIX)]
    if len(existing_saved) == 0:
        ANSI.print_col("None found", ANSI.DARK_GRAY)
        latest_id = 0
    else:
        ANSI.print_col("Backups found:\t\n" + "\t\n".join([f.name for f in existing_saved]), ANSI.DARK_GRAY)
        latest_id = max([int(f.name.split(CLOUD_FOLDER_PREFIX)[1]) for f in existing_saved])
    print()

    ANSI.print_col("Uploading folder...", ANSI.CYAN)
    new_folder_name = CLOUD_FOLDER_PREFIX + str(latest_id + 1)
    cloud.upload_folder(configparser.get(SavedFolderConfig.NAME), new_folder_name, root_folder.cloud_id)
    ANSI.print_col(f"Folder uploaded successfully with name {new_folder_name}", ANSI.DARK_GRAY)
    print()

    ANSI.print_col("Checking to delete old backups...", ANSI.CYAN)
    if len(existing_saved) + 1 <= configparser.get(MaxBackupConfig.NAME):
        ANSI.print_col("Max backups not exceeded, skipping...", ANSI.DARK_GRAY)
    else:
        oldest_folder = min(existing_saved, key=lambda s: s.created_time)
        ANSI.print_col(f"Removing {oldest_folder.name}...", ANSI.DARK_GRAY)
        cloud.delete_cloud_file(oldest_folder.cloud_id)
        ANSI.print_col(f"{oldest_folder.name} removed successfully.", ANSI.DARK_GRAY)
    print()
