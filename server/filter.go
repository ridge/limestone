package server

import "github.com/ridge/limestone/wire"

func compileFilter(filter wire.Filter) func(wire.Changes) wire.Changes {
	if filter == nil {
		return func(changes wire.Changes) wire.Changes {
			return changes
		}
	}

	prepared := map[string]map[string]bool{} // kind -> prop -> true || kind -> nil
	for kind, props := range filter {
		if props == nil {
			prepared[kind] = nil
		} else {
			p := map[string]bool{}
			for _, name := range props {
				p[name] = true
			}
			prepared[kind] = p
		}
	}

	return func(changes wire.Changes) wire.Changes {
		res := wire.Changes{}
		for kind, byID := range changes {
			props, ok := prepared[kind]
			switch {
			case !ok:
				continue
			case props == nil:
				res[kind] = byID
			default:
				newByID := wire.KindChanges{}
				for id, diff := range byID {
					newDiff := wire.Diff{}
					for prop, value := range diff {
						if props[prop] {
							newDiff[prop] = value
						}
					}
					if len(newDiff) != 0 {
						newByID[id] = newDiff
					}
				}
				if len(newByID) != 0 {
					res[kind] = newByID
				}
			}
		}

		if len(res) == 0 {
			return nil
		}
		return res
	}
}
