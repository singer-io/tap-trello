from tap_trello.streams.actions import Actions
from tap_trello.streams.boards import Boards
from tap_trello.streams.board_memberships import BoardMemberships
from tap_trello.streams.board_custom_fields import BoardCustomFields
from tap_trello.streams.board_labels import BoardLabels
from tap_trello.streams.cards import Cards
from tap_trello.streams.card_attachments import CardAttachments
from tap_trello.streams.card_custom_field_items import CardCustomFieldItems
from tap_trello.streams.checklists import Checklists
from tap_trello.streams.lists import Lists
from tap_trello.streams.members import Members
from tap_trello.streams.organizations import Organizations
from tap_trello.streams.organization_actions import OrganizationActions
from tap_trello.streams.organization_members import OrganizationMembers
from tap_trello.streams.organization_memberships import OrganizationMemberships
from tap_trello.streams.users import Users


STREAMS = {
    "actions": Actions,
    "boards": Boards,
    "board_memberships": BoardMemberships,
    "board_custom_fields": BoardCustomFields,
    "board_labels": BoardLabels,
    "cards": Cards,
    "card_attachments": CardAttachments,
    "card_custom_field_items": CardCustomFieldItems,
    "checklists": Checklists,
    "lists": Lists,
    "members": Members,
    "organizations": Organizations,
    "organization_actions": OrganizationActions,
    "organization_members": OrganizationMembers,
    "organization_memberships": OrganizationMemberships,
    "users": Users,
}
