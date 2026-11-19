Reject empty and dot-segment names in the deprecated Drive functions before
sending a request. URL normalization resolved them, so `delete_drive(name=".")`
addressed the route that deletes a sandbox named `drives`.
