import argparse

from dataset.dataset import generate_dataset, generate_datashop
from dataset.utils import guarentee_int

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="PySpark Job with Parameters")
    parser.add_argument("--bucket_name", required=True, help="S3 Bucket Name")
    parser.add_argument("--action", required=True, help="Dataformat Export Type")
    parser.add_argument("--job_id", required=True, help="Job id")
    parser.add_argument("--section_ids", required=True, help="Course Section Ids")
    parser.add_argument("--page_ids", required=True, help="Restrict to these page ids")
    parser.add_argument("--ignored_student_ids", required=False, help="Student Ids to Ignore")
    parser.add_argument("--chunk_size", required=True, help="Chunk Size")
    parser.add_argument("--sub_types", required=False, help="Event Sub Types")
    parser.add_argument("--exclude_fields", required=False, help="List of fields to exclude")
    parser.add_argument("--enforce_project_id", required=False, help="Project id to ensure the data is from this project")

    args = parser.parse_args()

    section_ids = [int(x) for x in args.section_ids.split(",")]
    ignored_student_ids = [int(x) for x in (args.ignored_student_ids.split(",") if args.ignored_student_ids else [])]
    sub_types = [x for x in (args.sub_types.split(",") if args.sub_types else [])]
    action = args.action
    bucket_name = args.bucket_name
    inventory_bucket_name = bucket_name + "-inventory"
    chunk_size = int(args.chunk_size)
    exclude_fields = [x for x in (args.exclude_fields.split(",") if args.exclude_fields else [])]
    
    if action == 'datashop' or args.page_ids == "all":
        page_ids = None
    else:
        page_ids = [int(x) for x in (args.page_ids.split(","))]

    project_id = args.enforce_project_id if args.enforce_project_id else None
    project_id = guarentee_int(project_id)

    context = {
        "bucket_name": bucket_name,
        "inventory_bucket_name": inventory_bucket_name,
        "job_id": args.job_id,
        "ignored_student_ids": ignored_student_ids,
        "chunk_size": chunk_size,
        "section_ids": section_ids,
        "page_ids": page_ids,
        "action": action,
        "sub_types": sub_types,
        "exclude_fields": exclude_fields,
        "project_id": project_id
    }

    action = args.action

    if action == 'datashop':
        generate_datashop(context)
    else:
        generate_dataset(section_ids, action, context)

    print("job completed")