"""Dumper YAML com estilo dbt: strings multilinha em bloco `>` (folded),
igual ao usado em todo o projeto (ver agentes_dbt/gold/schema.yml,
metadata/schema.yml). PyYAML por padrão não faz isso sozinho.
"""

from __future__ import annotations

import yaml


class _Folded(str):
    """Marca uma string para ser serializada em estilo `>` (folded block)."""


def _folded_representer(dumper: yaml.Dumper, data: _Folded) -> yaml.ScalarNode:
    # Termina com \n de propósito: força o PyYAML a escolher chomping "clip"
    # (`>`) em vez de "strip" (`>-`), igual à convenção já usada no projeto.
    text = str(data)
    if not text.endswith("\n"):
        text += "\n"
    return dumper.represent_scalar("tag:yaml.org,2002:str", text, style=">")


class DbtDumper(yaml.SafeDumper):
    def increase_indent(self, flow=False, indentless=False):
        # PyYAML por padrão não indenta itens de lista sob a chave-pai
        # (`sources:\n- name: x`); o resto do projeto usa `sources:\n  - name: x`.
        return super().increase_indent(flow, False)


DbtDumper.add_representer(_Folded, _folded_representer)


def folded(text: str) -> _Folded:
    return _Folded(text)


def dump_dbt_yaml(doc: dict, *, description_keys: tuple[str, ...] = ("description",)) -> str:
    """Serializa um dict pro estilo YAML do dbt, dobrando em `>` qualquer
    valor de string comprida sob uma chave em `description_keys`."""

    def _mark(obj):
        if isinstance(obj, dict):
            return {
                k: (folded(v) if k in description_keys and isinstance(v, str) and v.strip() else _mark(v))
                for k, v in obj.items()
            }
        if isinstance(obj, list):
            return [_mark(v) for v in obj]
        return obj

    marked = _mark(doc)
    return yaml.dump(
        marked,
        Dumper=DbtDumper,
        allow_unicode=True,
        sort_keys=False,
        width=90,
        default_flow_style=False,
        indent=2,
    )
