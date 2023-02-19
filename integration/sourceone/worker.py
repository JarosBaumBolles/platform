""" Sourceone Workers module"""

from collections import Counter
from io import StringIO
from pathlib import Path
from queue import Queue
from threading import Lock
from typing import Any, List, Optional

import pandas as pd
from dataclass_factory import Factory, Schema
from imap_tools import A, MailBox, MailMessageFlags
from pandas import DataFrame
from pendulum import DateTime

from common import settings as CFG
from common.data_representation.standardized.meter import Meter
from common.date_utils import format_date, parse, truncate
from integration.base_integration import BaseFetchWorker, BaseStandardizeWorker
from integration.sourceone.data_structures import DataFile


class FetchWorker(BaseFetchWorker):
    """Sourceone worker functionality"""

    __fetch_file_name_tmpl__ = "{base_file_name}_{idx}"
    __created_by__ = "Sourceone Fetch Worker"
    __description__ = "Sourceone Integration"
    __name__ = "Sourceone Fetch Worker"

    __raw_files_pool_size__ = 1000

    __mail_root_folder__ = Path("INBOX")
    __mail_sourceone_processed__ = "SOURCEONE_PROCESSED"

    def __init__(
        self,
        missed_hours: Queue,
        fetched_files: Queue,
        fetch_update: Queue,
        config: Any,
    ) -> None:
        super().__init__(
            missed_hours=missed_hours,
            fetched_files=fetched_files,
            fetch_update=fetch_update,
            config=config,
        )
        self._fetch_lock: Lock = Lock()
        self._raw_fetch_queue = Queue()
        self._fetch_counter = Counter()

        self._factory = Factory(
            default_schema=Schema(trim_trailing_underscore=False, skip_internal=False)
        )

    def _get_mail_connection(self) -> MailBox:
        mail_box = MailBox(self._config.host).login(
            self._config.email, self._config.password
        )
        return mail_box

    def _create_mailbox_folder(
        self, mailbox: MailBox, folder: str, logs: Queue, worker_idx: str
    ) -> None:
        is_exists = mailbox.folder.exists(folder)
        if not is_exists:
            mailbox.folder.create(folder)
        else:
            logs.put(
                (
                    "WARNING",
                    self._trace_id,
                    f"{self.__created_by__} - [{worker_idx}]. "
                    f"Mailbox folder {folder} alredy exist.",
                )
            )

    @staticmethod
    def _joinmailpath(*args):
        root = Path(args[0])
        return str(root.joinpath(*args[1:])).replace(
            root._flavour.sep, "|"  # pylint: disable=protected-access,no-member
        )

    def _get_mail_processed_folder(
        self, mailbox: MailBox, logs: Queue, worker_idx: str
    ) -> str:
        processed_folder = self._joinmailpath(
            self.__mail_root_folder__, self.__mail_sourceone_processed__
        )
        self._create_mailbox_folder(
            mailbox=mailbox, folder=processed_folder, logs=logs, worker_idx=worker_idx
        )
        return processed_folder

    def _get_message_query(self) -> A:
        return A(subject=self._config.subject, from_=self._config.sender, seen=False)

    def _move_processed_msgs(
        self,
        mailbox: MailBox,
        uids: List[int],
        folder: str,
        logs: Queue,
        worker_idx: str,
    ) -> None:
        if uids:
            mailbox.flag(uids, (MailMessageFlags.SEEN,), True)
            mailbox.move(uids, folder)
            logs.put(
                (
                    "INFO",
                    self._trace_id,
                    f"Moved '{','.join(uids)}' to the folder {folder}"
                    f"Worker idx {worker_idx}",
                )
            )

    def _mark_as_unread_skipped_msgs(
        self, mailbox: MailBox, uids: List[int], logs: Queue, worker_idx: str
    ) -> None:
        if uids:
            mailbox.flag(uids, (MailMessageFlags.SEEN,), False)
            logs.put(
                (
                    "INFO",
                    self._trace_id,
                    f"Prepared'{','.join(uids)}' to next fetch iteration"
                    f"Worker idx {worker_idx}",
                )
            )

    def filter_mail_attachments(self, attachemnts: List) -> List:
        """Filter mail attachemts to drop excess"""
        return list(
            filter(lambda x: x.filename == self._config.attachment, attachemnts)
        )

    def run_fetch_worker(self, logs: Queue, worker_idx: str) -> None:
        processed, skipped = set(), set()
        processing = True
        counter = Counter()
        data_blobs = []
        with self._get_mail_connection() as mailbox:
            query = self._get_message_query()

            for msg in mailbox.fetch(query):
                if processing:
                    attachements = self.filter_mail_attachments(msg.attachments)
                    expected_cnt = counter["1"] + len(attachements)
                    if expected_cnt >= self.__raw_files_pool_size__:
                        processing = False
                        skipped.add(msg.uid)
                        continue

                    data_blobs += list(map(lambda x: x.payload, attachements))
                    processed.add(msg.uid)
                else:
                    skipped.add(msg.uid)
            self._move_processed_msgs(
                mailbox=mailbox,
                uids=processed,
                folder=self._get_mail_processed_folder(mailbox, logs, worker_idx),
                logs=logs,
                worker_idx=worker_idx,
            )

            self._mark_as_unread_skipped_msgs(
                mailbox=mailbox, uids=skipped, logs=logs, worker_idx=worker_idx
            )

        self._unbundle_blobs(raw_blobs=data_blobs, logs=logs, worker_idx=worker_idx)

    def _unbundle_blobs(  # pylint:disable=unused-argument
        self, raw_blobs: List[str], logs: Queue, worker_idx: str
    ) -> None:
        """Fetch Post Processing"""

        if self._missed_hours_queue.empty():
            logs.put(
                (
                    "WARNING",
                    self._trace_id,
                    "Missed hours queue is empty. Maybe data up to date.",
                )
            )
            return None

        empty_run_count = 0

        while True:
            if self._missed_hours_queue.empty():
                if empty_run_count == 3:
                    break
                empty_run_count += 1
            else:
                raw_object = self._missed_hours_queue.get()
                for blob in raw_blobs:
                    file_name = self.__fetch_file_name_tmpl__.format(
                        base_file_name=raw_object.file_name,
                        idx=self._fetch_counter["file_id"],
                    )
                    self._fetch_counter["file_id"] += 1
                    file_info = DataFile(
                        file_name=file_name,
                        bucket=self._config.extra.raw.bucket,
                        path=self._config.extra.raw.path,
                        body=blob,
                        cfg=raw_object.meter_cfg,
                        missed_hours=raw_object.meters_hours,
                    )
                    self._fetched_files_queue.put(file_info)
                    self._shadow_fetched_files_queue.put(file_info)
                    self._add_to_update(file_info, self._fetch_update_file_buffer)
                self._missed_hours_queue.task_done()


class StandardizeWorker(BaseStandardizeWorker):
    """Source Standardization Worker."""

    __created_by__ = "Sourceone Connector"

    @staticmethod
    def _standartize_generic(row, run_time, meter_uri, created_by) -> str:
        meter = Meter()
        meter.created_date = run_time
        meter.start_time = row.start_date
        meter.end_time = row.end_date
        meter.created_by = created_by
        meter.usage = row.Consumption
        meter.meter_uri = meter_uri

        return meter.as_str()

    def _standardize_electric(
        self, data: str, mtr_dates: List[DateTime], mtr_cfg: Any
    ) -> Optional[DataFrame]:

        data_df = pd.read_csv(StringIO(data), sep=",", header=1)

        data_df.rename(
            columns={"Date": "DateTime", "Consumption (kWh)": "Consumption"},
            inplace=True,
        )

        data_df[["start_date", "end_date"]] = list(
            map(self.date_repr, data_df.DateTime)
        )

        mtr_dates = list(map(lambda x: self.date_repr(x)[0], mtr_dates))
        data_df = data_df[data_df.start_date.isin(mtr_dates)]
        if len(data_df) == 0:
            return []

        data_df["meter"] = data_df.apply(
            self._standartize_generic,
            axis=1,
            args=(self._run_time, mtr_cfg.meter_uri, self.__created_by__),
        )
        return data_df.reset_index()

    @staticmethod
    def date_repr(dt_str: str) -> str:
        """Get date string reprezentation"""
        start_date = truncate(parse(dt_str, tz_info="UTC"), level="hour")
        end_date = start_date.add(minutes=59, seconds=59)

        start_date = format_date(start_date, CFG.PROCESSING_DATE_FORMAT)
        end_date = format_date(end_date, CFG.PROCESSING_DATE_FORMAT)
        return start_date, end_date

    def _standardize(self, raw_file_obj: DataFile) -> List[DataFile]:
        """Standardize the given raw file"""
        meter_type = raw_file_obj.cfg.type.strip().lower().replace(" ", "_")
        stndrdz_mthd_nm = f"_standardize_{meter_type}"
        stndrdz_func = getattr(self, stndrdz_mthd_nm, "")
        if not callable(stndrdz_func):
            raise RuntimeError(
                f"Cannot find standadize processor for the given "
                f"meter type {meter_type}. Skipping"
            )

        meters_df = stndrdz_func(
            data=raw_file_obj.body.decode("utf-8"),
            mtr_dates=raw_file_obj.missed_hours,
            mtr_cfg=raw_file_obj.cfg,
        )

        standardized_files = []

        for _, meter in meters_df.iterrows():
            standardized_files.append(
                DataFile(
                    file_name=meter.start_date,
                    bucket=raw_file_obj.cfg.standardized.bucket,
                    path=raw_file_obj.cfg.standardized.path,
                    body=meter.meter,
                    cfg=raw_file_obj.cfg,
                )
            )

        return standardized_files
