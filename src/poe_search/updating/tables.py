from abc import ABC

import pandas as pd

from .updates import Updater, WikiTablePull, WikiApiFormatting, PsqlTableMetaData, WikiTableMetaData
from ..wiki_api.pull import pull_image_url


class TableUpdater(ABC):

    def __init__(self,
                 psql_table_metadata: PsqlTableMetaData,
                 wiki_table_metadata: WikiTableMetaData):
        self._psql_meta = psql_table_metadata
        self._wiki_meta = wiki_table_metadata

        self._psql_df = None

    def _format_df_for_upsert(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def _fetch_formatted_wiki_dataframe(self) -> pd.DataFrame:
        data = WikiTablePull(
            table_name=self._wiki_meta.table_name,
            fields=self._wiki_meta.fields
        ).fetch_table_data()
        df = WikiApiFormatting.format_api_data(data)
        return df

    def upsert(self, updater: Updater):
        df = self._fetch_formatted_wiki_dataframe()
        df = self._format_df_for_upsert(df)

        image_col_name = self._wiki_meta.image_file_col_name
        if image_col_name:
            df[image_col_name] = df[image_col_name].apply(pull_image_url)

        updater.update_sql(
            wiki_df=df,
            wiki_table_metadata=self._wiki_meta,
            psql_table_metadata=self._psql_meta
        )

        self._psql_df = df

    def insert_into_text_index(self,
                               index_ids: list,
                               texts: list):
        text_col_name = self._psql_meta.text_col_name
        if not text_col_name:
            return

        id_col_name = self._psql_meta.id_col_name
        ids = list(self._psql_df[id_col_name].apply(lambda id_: f"{id_}_@{self._psql_meta.table_name}"))

        text = list(self._psql_df[text_col_name])

        index_ids.extend(ids)
        texts.extend(text)


class SkillsTableUpdater(TableUpdater):

    def __init__(self):
        super().__init__(
            wiki_table_metadata=WikiTableMetaData(
                table_name='skill',
                fields=['_pageName=page_name', 'skill_icon', 'skill_id', 'stat_text'],
                id_col_name='skill_id',
                image_file_col_name='skill_icon'
            ),
            psql_table_metadata=PsqlTableMetaData(
                table_name='skills',
                fields=['skill_name', 'image_file_name', 'id', 'skill_text'],
                id_col_name='id'
            )
        )


class SkillQualitiesTable(TableUpdater):

    def __init__(self):
        super().__init__(
            psql_table_metadata=PsqlTableMetaData(
                table_name='skill_qualities',
                fields=['skill_name', 'quality_text'],
                id_col_name='skill_name'
            ),
            wiki_table_metadata=WikiTableMetaData(
                table_name='skill_quality',
                fields=['_pageName=page_name', 'stat_text']
            )
        )


class ItemStatsTable(TableUpdater):

    def __init__(self):
        super().__init__(
            psql_table_metadata=PsqlTableMetaData(
                table_name='item_stats',
                fields=['id', 'item_name', 'stat_id'],
                id_col_name='skill_name',
                text_col_name='quality_text'
            ),
            wiki_table_metadata=WikiTableMetaData(
                table_name='item_stats',
                fields=['_pageName=page_name', 'id', 'mod_id']
            )
        )

    def _format_df_for_upsert(self, df: pd.DataFrame) -> pd.DataFrame:
        df = (
            df
            .reset_index()
            .rename(columns={
                'index': 'id',
                'page_name': 'item_name'
            })
        )
        return df


class ModsTable(TableUpdater):

    def __init__(self):
        super().__init__(
            wiki_table_metadata=WikiTableMetaData(
                table_name='mods',
                fields=['id', 'name', 'stat_text_raw']
            ),
            psql_table_metadata=PsqlTableMetaData(
                table_name='mods',
                fields=['id', 'name', 'stat_text', 'mod_groups'],
                id_col_name='id',
                text_col_name='stat_text'
            )
        )

    def _format_df_for_upsert(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['mod_groups'] != 'Nothing']
        df = df.rename(columns={'stat_text_raw': 'stat_text'})
        return df


class ItemBuffsTable(TableUpdater):
    super().__init__(
        wiki_table_metadata=WikiTableMetaData(
            table_name='item_buffs',
            fields=['buff_values', 'id', 'stat_text', 'icon'],
            image_file_col_name='icon',
            id_col_name='id'
        ),
        psql_table_metadata=PsqlTableMetaData(
            table_name='item_buffs',
            fields=['buff_values', 'id', 'stat_text', 'image_file_name'],
            id_col_name='id'
        )
    )


class CorpseItemsTable(TableUpdater):
    super().__init__(
        wiki_table_metadata=WikiTableMetaData(
            table_name='corpse_items',
            fields=['_pageName=page_name', 'monster_abilities']
        ),
        psql_table_metadata=PsqlTableMetaData(
            table_name='corpse_items',
            fields=['item_name', 'monster_abilities']
        )
    )

