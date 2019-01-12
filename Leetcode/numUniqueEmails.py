def numUniqueEmails(emails):
    out=set()
    for email in emails:
        local, domain = email.split('@')
        # excluding +
        local = local[:local.index('+')]
        #replace '.'
        local=local.replace('.','')
        out.add(local+'@'+domain)
    print(out)
    return len(out)


if __name__ == '__main__':
    print(numUniqueEmails(["test.email+alex@leetcode.com","test.e.mail+bob.cathy@leetcode.com","testemail+david@lee.tcode.com"]))