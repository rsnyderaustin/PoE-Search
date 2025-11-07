
import html
import re
import pandas as pd

from src.poe_search.wiki_api.pull import WikiTablePull, WikiImageUrlPull


class DatabaseUpdate:

    @staticmethod
    def _determine_image_url(file_name: str):
        return WikiImageUrlPull(file_name).fetch_image_url()


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

def update_database():
    skills_df = update_skills()

def update_skills():
    data = WikiTablePull(
        table_name='skill',
        fields=['_pageName=page_name', 'skill_icon', 'skill_id', 'stat_text']
    ).fetch_table_data()
    df = WikiApiFormatting.format_api_data(data)
    df['skill_icon'] = df['skill_icon'].apply(_determine_image_url)
    return df


def update_skill_qualities():
    data = WikiTablePull(
        table_name='skill_quality',
        fields=['_pageName=page_name', 'stat_text']
    ).fetch_table_data()
    df = WikiApiFormatting.format_api_data(data)
    return df


def update_item_stats():
    data = WikiTablePull(
        table_name='item_stats',
        fields=['_pageName=page_name', 'id']
    ).fetch_table_data()
    df = WikiApiFormatting.format_api_data(data)
    return df


def update_mods():
    data = WikiTablePull(
        table_name='mods',
        fields=['id', 'name', 'stat_text_raw']
    ).fetch_table_data()
    df = WikiApiFormatting.format_api_data(data)
    return df


def determine_synthesis_mod_ids():
    data = WikiTablePull(
        table_name='synthesis_corrupted_mods',
        fields=['mod_ids']
    ).fetch_table_data()
    df = WikiApiFormatting.format_api_data(data,
                                           split_comma_cols={'mod_ids'})
    return df


def update_item_buffs():
    data = WikiTablePull(
        table_name='item_buffs',
        fields=['buff_values', 'id', 'stat_text']
    ).fetch_table_data()
    df = WikiApiFormatting.format_api_data(data)
    return df


def update_corpse_items():
    data = WikiTablePull(
        table_name='corpse_items',
        fields=['_pageName=page_name', 'monster_abilities']
    ).fetch_table_data()
    df = WikiApiFormatting.format_api_data(data)
    return df


def determine_synthesis_global_mods():
    data = WikiTablePull(
        table_name='synthesis_global_mods',
        fields=['mod_id']
    ).fetch_table_data()
    df = WikiApiFormatting.format_api_data(data)
    return df


def update_pantheon_souls():
    data = WikiTablePull(
        table_name='pantheon_souls',
        fields=['id', 'name', 'stat_text']
    ).fetch_table_data()
    df = WikiApiFormatting.format_api_data(data)
    return df


def determine_synthesis_mods():
    data = WikiTablePull(
        table_name='synthesis_mods',
        fields=['mod_ids']
    ).fetch_table_data()
    df = WikiApiFormatting.format_api_data(data)
    return df


def update_mastery_effects():
    data = WikiTablePull(
        table_name='mastery_effects',
        fields=['stat_ids', 'stat_text_raw']
    ).fetch_table_data()
    df = WikiApiFormatting.format_api_data(data)
    return df


def update_passive_skills():
    data = WikiTablePull(
        table_name='passive_skills',
        fields=['name', 'stat_text', 'icon']
    ).fetch_table_data()
    df = WikiApiFormatting.format_api_data(data)
    df['icon'] = df['icon'].apply(WikiApiFormatting.determine_image_url)
    return df


def determine_crafting_mods():
    data = WikiTablePull(
        table_name='crafting_bench_options',
        fields=['item_class_categories', 'mod_id']
    ).fetch_table_data()
    df = WikiApiFormatting.format_api_data(
        data=data,
        split_comma_cols={'item_class_categories'}
    )
    return df


def determine_graft_skill_ids():
    data = WikiTablePull(
        table_name='grats',
        fields=['skill_id']
    ).fetch_table_data()
    df = WikiApiFormatting.format_api_data(data)
    return df
