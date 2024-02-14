import type { GitIdentities, Member } from "@dxta/extract-schema";
import Fuse from "fuse.js";

const extractUserFromEmail = (email: string | null) => {
  if (email) {
    const [name] = email.split('@');
    return name;
  }
  return null;
};

export function fuzzySearch(gitIdentities: Pick<GitIdentities, 'id' | 'name' | 'email'>[], members: Pick<Member, 'id' | 'name' | 'username' | 'email'>[]) {
  const identities = gitIdentities.map((identity) => ({ name: identity.name, email: identity.email, username: extractUserFromEmail(identity.email) }));

  const fuse = new Fuse(identities, {
    keys: ['username', 'name', 'email'],
    threshold: 0.20,
    location: 0,
    distance: 100,
    includeScore: true,
    useExtendedSearch: true,
  });


  const memberMap = new Map<number, {
    memberId: number,
    score: number,
  }>();

  for (const member of members) {
    const searchName = member.name ? member.name.replace(' ', '|') : null
    const searchUserName = member.username ? member.username.replace(' ', '|') : null;
    const searchEmail = member.email ? member.email.replace(' ', '|') : null;
    const searchResult = fuse.search([searchName, searchUserName, searchEmail].filter(t => t !== null).join('|'), { limit: 5 });

    for (const r of searchResult) {
      const { score, refIndex } = r;

      const gitIdentity = gitIdentities.at(refIndex);

      if (gitIdentity && memberMap.has(gitIdentity.id) && score) {
        const currentScore = memberMap.get(gitIdentity.id)?.score;
        if (currentScore && currentScore > score) {
          memberMap.set(gitIdentity.id, {
            memberId: member.id,
            score,
          });
        }
      } else if (gitIdentity && score) {
        memberMap.set(gitIdentity.id, {
          memberId: member.id,
          score,
        });
      }
    }
  }

  const result: Map<number, number[]> = new Map();
  memberMap.forEach((value, key) => {
    if (result.has(value.memberId)) {
      const current = result.get(value.memberId);
      if (current) {
        current.push(key);
        current.sort();
      }
    } else {
      result.set(value.memberId, [key]);
    }
  });
  return result;
}
