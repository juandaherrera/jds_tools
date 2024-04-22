import os

import pytest

from jds_tools.hooks import JinjaHook


# Fixture to create a temporary directory with a sample template file
@pytest.fixture
def template_dir(tmp_path):
    templates = tmp_path / "templates"
    templates.mkdir()
    template_file = templates / "test_template.html"
    template_file.write_text("Hello {{ name }}!")
    return templates


@pytest.mark.unit
def test_jinja_hook_initialization(template_dir):
    """Test initialization and environment setting of JinjaHook."""
    hook = JinjaHook(str(template_dir))
    assert hook.templates_path == str(template_dir)
    assert hook.environment is not None


@pytest.mark.unit
def test_jinja_hook_invalid_path():
    """Test initialization with an invalid path."""
    with pytest.raises(ValueError):
        JinjaHook("/invalid/path")


@pytest.mark.unit
def test_template_rendering(template_dir):
    """Test rendering of a template."""
    hook = JinjaHook(str(template_dir))
    output = hook.render("test_template.html", {"name": "World"})
    assert output == "Hello World!"


@pytest.mark.unit
def test_template_not_found(template_dir):
    """Test rendering with a template that does not exist."""
    hook = JinjaHook(str(template_dir))
    with pytest.raises(Exception) as e_info:
        hook.render("non_existent_template.html", {})
