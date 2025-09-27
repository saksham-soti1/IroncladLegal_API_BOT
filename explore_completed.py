import json
import os
from ironclad_api import list_workflows, get_workflow, _get

def main():
    # Create an output folder for JSON dumps
    os.makedirs("workflow_dumps", exist_ok=True)

    # Step 1: list 5 completed workflows
    data = list_workflows(status="completed", page_size=10)
    workflows = data.get("list", [])

    print(f"Found {len(workflows)} completed workflows\n")

    # Step 2: dump raw metadata for each workflow
    for wf in workflows:
        wf_id = wf.get("id")
        readable_id = wf.get("ironcladId") or wf.get("readableId") or wf.get("id")
        title = wf.get("title", "(no title)")

        print("=" * 80)
        print(f"Workflow ID: {wf_id} | Title: {title}")
        print("=" * 80)

        # Collect everything in one dict
        all_data = {}

        # Full workflow
        try:
            full = get_workflow(wf_id)
            all_data["workflow"] = full
        except Exception as e:
            all_data["workflow_error"] = str(e)

        # Participants
        try:
            participants = _get(f"/workflows/{wf_id}/participants")
            all_data["participants"] = participants
        except Exception as e:
            all_data["participants_error"] = str(e)

        # Comments
        try:
            comments = _get(f"/workflows/{wf_id}/comments")
            all_data["comments"] = comments
        except Exception as e:
            all_data["comments_error"] = str(e)

        # Write full dump to file
        filename = f"workflow_dumps/{readable_id or wf_id}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False)

        print(f"✅ Dumped workflow {readable_id} → {filename}")

if __name__ == "__main__":
    main()
