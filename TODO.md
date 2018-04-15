Support AFTER ROW triggers
--------------------------

Per 9.4 release notes:

    Foreign data wrappers that support updating foreign tables must consider the possible presence of AFTER ROW triggers (Noah Misch)

    When an AFTER ROW trigger is present, all columns of the table must be returned by updating actions, since the trigger might inspect any or all of them. Previously, foreign tables never had triggers, so the FDW might optimize away fetching columns not mentioned in the RETURNING clause (if any).
