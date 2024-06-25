
1. change this line
    
    ```go
    func downloadRules(destiantion string) {
        namespaces := map[string]string{}
    }
    ```
    into something like
    
    ```go
    func downloadRules(destiantion string) {
        namespaces := map[string]string{
            "cortex-dev-01": "dev-us-central-0",
        }
    }
    ```

2. run 

    ```bash
    go run main.go download /tmp/destination
    ```

3. run 

    ```bash
    go run main.go analyze /tmp/destination
    ```
