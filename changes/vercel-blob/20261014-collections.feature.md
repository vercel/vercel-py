Add lazy query_blobs and query_blob_entries iterators that automatically follow
cursors, including typed virtual folder prefixes, server-side copy_blob with
source ETag preconditions, and batch delete_blob of up to 1,000 objects with
single-object ETag preconditions in both async and sync APIs.
