import html
import re
import pandas as pd

from src.poe_search.wiki_api.pull import WikiTablePull, WikiImageUrlPull
from src.poe_search.psql.manager import PsqlManager


class PsqlTableMetaData:

    def __init__(self,
                 table_name: str,
                 fields: list[str],
                 id_col_name: str,
                 text_col_name: str = None):
        self.table_name = table_name
        self.fields = fields

        self.id_col_name = id_col_name

        self.text_col_name = text_col_name


class WikiTableMetaData:

    def __init__(self,
                 table_name: str,
                 fields: list[str],
                 id_col_name: str = None,
                 image_file_col_name: str = None):
        self.table_name = table_name
        self.fields = fields

        self.id_col_name = id_col_name
        self.image_file_col_name = image_file_col_name

class WikiApiFormatting:

    @staticmethod
    def _format_api_string(
            s: str,
            split_commas: bool = False
    ) -> list[str]:
        if not s:
            return []

        s = html.unescape(s)

        if '<br>' in s:
            s = s.split('<br>')

        if split_commas and not isinstance(s, list):
            s = s.split(',')

        s = [s] if not isinstance(s, list) else s

        return s

    @classmethod
    def format_api_data(
            cls,
            data: list,
            split_comma_cols: set[str] = None
    ) -> pd.DataFrame:
        split_comma_cols = split_comma_cols or []

        cols = list(data[0]['title'].keys())
        formatted_cols_map = {
            col: col.replace(' ', '_')
            for col in cols
        }
        return_d = {formatted_cols_map[col]: [] for col in cols}

        data = [d['title'] for d in data]
        for d in data:
            for col in cols:
                val = cls._format_api_string(
                    d[col],
                    split_commas=formatted_cols_map[col] in split_comma_cols
                )

                return_d[formatted_cols_map[col]].append(val)

        return pd.DataFrame(return_d)


class Updater:

    def __init__(self,
                 psql_manager: PsqlManager):
        self._psql_manager = psql_manager

    def _update_corpse_items(self) -> pd.DataFrame:
        data = WikiTablePull(
            table_name='corpse_items',
            fields=['_pageName=page_name', 'monster_abilities']
        ).fetch_table_data()
        df = WikiApiFormatting.format_api_data(data)
        df = df.rename(columns={'page_name': 'item_name'})
        self._update_sql(
            wiki_df=df,
            wiki_df_id_col='item_name',
            psql_table_name='corpse_items'
        )

        return df

    def _update_pantheon_souls(self) -> pd.DataFrame:
        data = WikiTablePull(
            table_name='pantheon_souls',
            fields=['id', 'name', 'stat_text', 'target_area_id']
        ).fetch_table_data()
        df = WikiApiFormatting.format_api_data(data)
        df = df.rename(
            columns={
                'id': 'pantheon_name',
                'name': 'enemy_name',
                'target_area_id': 'location_name'
            }
        )
        df['id'] = f"{df['pantheon_name']}_{df['enemy_name']}"
        df['location_name'] = df['location_name'].apply(lambda name: name.replace('MapWorlds', ''))
        self._update_sql(
            wiki_df=df,
            wiki_df_id_col='id',
            psql_table_name='pantheon_souls'
        )

        return df

    def _update_mastery_effects(self) -> pd.DataFrame:
        data = WikiTablePull(
            table_name='mastery_effects',
            fields=['id', 'stat_ids', 'stat_text_raw']
        ).fetch_table_data()
        df = WikiApiFormatting.format_api_data(data)
        self._update_sql(
            wiki_df=df,
            wiki_df_id_col='id',
            psql_table_name='mastery_effects'
        )

        return df

    def _update_passive_skills(self) -> pd.DataFrame:
        data = WikiTablePull(
            table_name='passive_skills',
            fields=['id', 'name', 'stat_text', 'icon']
        ).fetch_table_data()
        df = WikiApiFormatting.format_api_data(data)
        df['image_file_name'] = df['icon'].apply(
            lambda file_name: WikiImageUrlPull(file_name).fetch_image_url()
            if file_name else None
        )
        self._update_sql(
            wiki_df=df,
            wiki_df_id_col='id',
            psql_table_name='passive_skills'
        )

        return df

    def _update_crafting_mods(self) -> pd.DataFrame:
        invalid_item_classes = {
            'Map',
            'Map Fragment',
            'Breachstone'
        }
        data = WikiTablePull(
            table_name='crafting_bench_options',
            fields=['id', 'item_class_categories', 'mod_id']
        ).fetch_table_data()
        df = WikiApiFormatting.format_api_data(
            data=data,
            split_comma_cols={'item_class_categories'}
        )
        df = df[df['item_class_categories'].apply(lambda categories: set(categories).isdisjoint(invalid_item_classes))]
        self._update_sql(
            wiki_df=df,
            wiki_df_id_col='id',
            psql_table_name='crafting_mods'
        )

        return df

    @staticmethod
    def _insert_sources(name: str,
                        insert_values: list,
                        entry_sources: list,
                        entry_ids: list):
        if isinstance(insert_values[0], list):
            vals = [v for v_list in insert_values for v in v_list]
        else:
            vals = insert_values

        entry_sources.extend([name for _ in range(len(vals))])
        entry_ids.extend(vals)

    def _update_mod_id_sources(self) -> pd.DataFrame:
        mod_id_sources = {
            'source': [],
            'mod_id': []
        }
        data = WikiTablePull(
            table_name='synthesis_global_mods',
            fields=['mod_id']
        ).fetch_table_data()
        synthesis_global_mod_ids_df = WikiApiFormatting.format_api_data(data)
        self._insert_sources(
            name='synthesis_global_mod',
            insert_values=list(synthesis_global_mod_ids_df['mod_id']),
            entry_sources=mod_id_sources['source'],
            entry_ids=mod_id_sources['mod_id']
        )

        data = WikiTablePull(
            table_name='synthesis_corrupted_mods',
            fields=['mod_ids']
        ).fetch_table_data()
        synthesis_corrupted_mod_ids_df = WikiApiFormatting.format_api_data(
            data,
            split_comma_cols={'mod_ids'}
        )
        self._insert_sources(
            name='synthesis_corrupted_mod',
            insert_values=list(synthesis_corrupted_mod_ids_df['mod_ids']),
            entry_sources=mod_id_sources['source'],
            entry_ids=mod_id_sources['mod_id']
        )

        data = WikiTablePull(
            table_name='synthesis_mods',
            fields=['mod_ids']
        ).fetch_table_data()
        synthesis_mod_ids_df = WikiApiFormatting.format_api_data(data)
        self._insert_sources(
            name='synthesis_mod',
            insert_values=list(synthesis_mod_ids_df['mod_ids']),
            entry_sources=mod_id_sources['source'],
            entry_ids=mod_id_sources['mod_id']
        )

        df = pd.DataFrame(mod_id_sources)
        self._update_sql(
            wiki_df=df,
            wiki_df_id_col='mod_id',
            psql_table_name='mod_id_sources'
        )

        return df

    def _update_skill_id_sources(self):
        skill_id_sources = {
            'skill_id': [],
            'source': []
        }

        data = WikiTablePull(
            table_name='grats',
            fields=['skill_id']
        ).fetch_table_data()
        graft_skill_ids_df = WikiApiFormatting.format_api_data(data)
        self._insert_sources(
            name='graft skill',
            insert_values=list(graft_skill_ids_df['skill_id']),
            entry_sources=skill_id_sources['source'],
            entry_ids=skill_id_sources['skill_id']
        )

        df = pd.DataFrame(skill_id_sources)
        self._update_sql(
            wiki_df=df,
            wiki_df_id_col='skill_id',
            psql_table_name='skill_id_sources'
        )

        return df

    def update_sql(self,
                   wiki_df: pd.DataFrame,
                   wiki_table_metadata: WikiTableMetaData,
                   psql_table_metadata: PsqlTableMetaData):

        old_hash = self._psql_manager.fetch_table_hash(psql_table_name=psql_table_metadata.table_name)
        if not old_hash:
            old_df = self._psql_manager.fetch_table_data(psql_table_name=psql_table_metadata.table_name)
            old_hash = self._psql_manager.hash_df(old_df)

        new_hash = self._psql_manager.hash_df(wiki_df)

        if old_hash != new_hash:
            self._psql_manager.update_table(
                psql_table_name=psql_table_metadata.table_name,
                new_df=wiki_df,
                new_df_id_col_name=wiki_table_metadata.id_col_name
            )

    def update(self):
        self._update_skills()
        self._update_skill_qualities()
        self._update_item_stats()
        self._update_mods()
        self._update_item_buffs()
        self._update_corpse_items()
        self._update_pantheon_souls()
        self._update_mastery_effects()
        self._update_passive_skills()
        self._update_crafting_mods()
