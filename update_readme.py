from cninfo_service import build_readme_content


def update_readme() -> None:
    readme_content = build_readme_content(csv_file="announcements.csv", days=7)
    with open("README.md", "w", encoding="utf-8") as file:
        file.write(readme_content)


if __name__ == "__main__":
    update_readme()
