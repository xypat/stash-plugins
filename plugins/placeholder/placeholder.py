import json


def main() -> None:
    print(
        json.dumps(
            {
                "error": None,
                "output": {
                    "message": "placeholder",
                },
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
