import sys
import os

posthog_key = os.environ.get('NEXT_PUBLIC_POSTHOG_KEY', '')
posthog_host = os.environ.get('NEXT_PUBLIC_POSTHOG_HOST', 'https://app.posthog.com')


sys.path.insert(0, os.path.abspath("../src"))

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Taylor AI"
copyright = "Taylor AI, Inc., 2023"
author = "Benjamin Anderson & Brian Kim"

html_favicon = "_static/favicon.ico"


# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",  # Automatic documentation from docstrings
    "sphinx.ext.viewcode",  # Add a link to the Python source code for classes, functions etc.
    "sphinx.ext.napoleon",  # Support for NumPy and Google style docstrings
    "sphinx_autodoc_typehints",  # Automatically document param types (less noise in class signature)
    "sphinx_copybutton",  # Add copy button to code blocks
    "sphinx_favicon",  # Add favicon
]

html_context = {
    "extra_html": f"""
    <script>
        // PostHog script with environment variables
        !function(t,e){...} // Your existing PostHog initialization script
        posthog.init('{posthog_key}',{{api_host:'{posthog_host}'}})
    </script>
    """
}

# Ensure that the extra HTML is added to every page
def setup(app):
    app.add_config_value("extra_html", "", "html")


templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
autodoc_typehints = "both"
autodoc_typehints_format = "fully-qualified"

highlight_language = "python"

html_theme_options = {
    "show_navbar_depth": 2,
    "repository_url": "https://github.com/taylorai/taylor-pipelines",
    "use_issues_button": False,
}

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_book_theme"
html_static_path = ["_static"]
