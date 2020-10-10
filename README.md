### asg-az-update

Avoid launching instances in faulty/unwanted Availability Zone

This allows you to `blacklist` or `whitelist` AZs for services with provided ASG prefix

Example:

```
AWS_PROFILE=<profile> python3 asg-az-update.py --services=<asg-prefix> --blacklist-az=us-east-1d --dryrun
```

### What is my AZ?

Run the following got check the mapping:

`AWS_PROFILE=<aws-profile> ./aws-az-map.sh us-east-1`
