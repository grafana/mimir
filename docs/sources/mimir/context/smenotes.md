Jan 13, 2026
Éamon / Taylor
Invited Éamon Ryan Taylor Cole
Attachments Éamon / Taylor Éamon / Taylor - 2026/01/13 14:29 PST - Recording 
Meeting records Recording 

Summary
Éamon Ryan and Taylor Cole discussed three key documentation issues, including a navigation loop when searching for "deploy memory version 3," which Taylor Cole agreed should be fixed to direct users to the Helmchart docs immediately. They also addressed the embedded Minio chart deployment failure due to nonexistent release tags, with Éamon Ryan suggesting using the `latest` tag as a temporary fix until Minio is removed. Finally, Éamon Ryan highlighted documentation errors regarding YAML indentation under "generate test metrics" and an incorrect URL for the Mimir data source, specifying that the URL should be `mimir-gateway` instead of the nonfunctional `mimir-ineX`.

Details
Documentation Loop and Navigation Éamon Ryan noted a problem where the initial online search for "deploy memory version 3" leads to a page instructing users to deploy Grafana memory in microservices mode using Kubernetes and the Mimir distributed helmchart. Following the subsequent link, however, redirects the user in a loop: from the docs to GitHub and back to a different docs page—the Helmchart docs—without providing deployment instructions. Taylor Cole agreed that the initial documentation should directly send users to the Helmchart docs.
Minio Chart Deployment Issues Éamon Ryan identified that the embedded Minio chart deployment fails because it attempts to pull releases that no longer exist, leading to deploy failure. Taylor Cole mentioned this might be related to an ongoing issue with Minio. Éamon Ryan proposed that although Minio should eventually be removed, the current version should function, suggesting that replacing the old release tag (e.g., from November 2024) with `latest` in the documentation or during override resolves the issue.
YAML Indentation and URL Inaccuracy Éamon Ryan pointed out two documentation errors related to configuration and data sources. First, the last line under "generate test metrics" needs to be indented by two spaces because YAML is particular about indentation, making the current lack of indentation invalid. Second, when adding the Mimir data source, the documentation currently suggests using `mimir-ineX` as the URL, which does not work; the correct URL is `mimir-gateway`. Éamon Ryan speculated that `mimir-ineX` might be an older reference that was consolidated into `mimir-gateway` in the chart, and the documentation was not updated. Despite these issues, Éamon Ryan confirmed that they were able to deploy and query Mimir after making these adjustments.

Suggested next steps
No suggested next steps were found for this meeting.

You should review Gemini's notes to make sure they're accurate. Get tips and learn how Gemini takes notes
Please provide feedback about using Gemini to take notes in a short survey.
