import logging
import os

from jinja2 import Environment, FileSystemLoader

from .base import BaseHook

logging.getLogger("Hooks")


class JinjaHook(BaseHook):
    """Manages a Jinja2 environment for rendering templates from a specified directory.

    This hook extends BaseHook and provides functionalities to load and render
    templates using Jinja2 from a specified filesystem path.

    Attributes:
        templates_path (str): The filesystem path to the directory containing Jinja2 templates.
    """

    def __init__(self, templates_path: str) -> None:
        """Initializes the JinjaHook with a specified path to the templates.

        Args:
            templates_path (str): A path to the directory containing Jinja2 templates.

        Raises:
            ValueError: If the templates_path does not exist or is not a directory.
        """
        self._templates_path = None
        self.templates_path = templates_path  # This calls the setter below

    @property
    def templates_path(self) -> str:
        """Gets the current template path."""
        return self._templates_path

    @templates_path.setter
    def templates_path(self, value: str) -> None:
        """Sets the template path and updates the Jinja environment.

        Validates that the specified path exists and is a directory before updating the environment.

        Args:
            value (str): The new path to set as the template path.

        Raises:
            ValueError: If the path does not exist or is not a directory.
        """
        if os.path.exists(value) and os.path.isdir(value):
            self._templates_path = value
            self._update_environment()
        else:
            raise ValueError("The specified path does not exist or is not a directory")

    def _update_environment(self) -> None:
        """Private method to update the Jinja environment based on the current template path."""
        try:
            self.environment = Environment(loader=FileSystemLoader(self._templates_path))
        except Exception as e:
            logging.error(f"Error trying to update Jinja Environment. Details: {e}")

    def render(self, template_file: str, params: dict = None) -> str:
        """Renders a template with the given parameters.

        Args:
            template_file (str): The name of the template file to render.
            params (dict, optional): A dictionary of parameters to pass to the template.

        Returns:
            str: The rendered template.
        """
        params = params or {}
        try:
            template = self.environment.get_template(template_file)
            return template.render(params)
        except Exception as e:
            logging.error(f"Error rendering template '{template_file}'. Details: {e}")
