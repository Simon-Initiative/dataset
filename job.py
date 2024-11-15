import argparse

from dataset.dataset import generate_dataset

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="PySpark Job with Parameters")
    parser.add_argument("--bucket_name", required=True, help="S3 Bucket Name")
    parser.add_argument("--action", required=True, help="Dataformat Export Type")
    parser.add_argument("--job_id", required=True, help="Job id")
    parser.add_argument("--section_ids", required=True, help="Course Section Ids")
    parser.add_argument("--ignored_student_ids", required=True, help="Student Ids to Ignore")
    parser.add_argument("--chunk_size", required=True, help="Chunk Size")
    parser.add_argument("--sub_types", required=True, help="Event Sub Types")
    parser.add_argument("--exclude_fields", required=False, help="List of fields to exclude")

    args = parser.parse_args()

    section_ids = [int(x) for x in args.section_ids.split(",")]
    ignored_student_ids = [int(x) for x in args.ignored_student_ids.split(",")]
    sub_types = [x for x in args.sub_types.split(",")]
    action = args.action
    bucket_name = args.bucket_name
    inventory_bucket_name = bucket_name + "-inventory"
    chunk_size = int(args.chunk_size)
    exclude_fields = [x for x in (args.exclude_fields.split(",") if args.exclude_fields else [])]

    context = {
        "bucket_name": bucket_name,
        "inventory_bucket_name": inventory_bucket_name,
        "job_id": args.job_id,
        "ignored_student_ids": ignored_student_ids,
        "chunk_size": chunk_size,
        "section_ids": section_ids,
        "action": action,
        "sub_types": sub_types,
        "exclude_fields": exclude_fields
    }
    action = args.action

    generate_dataset(section_ids, action, context)

    print("job completed")