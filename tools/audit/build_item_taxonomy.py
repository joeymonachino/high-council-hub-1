import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import app
import argparse
import hashlib
import json
import re

FILTER_OPTIONS = {
    "Offense", "Defense", "Hybrid", "Utility", "Starter",
    "Anti-Tank", "Anti-Heal", "Anti-Shield", "Cooldown", "Sustain", "Mobility", "Crowd Control", "Teamfight", "Economy",
    "Anti-Health", "Anti-Protections", "% Health Damage", "% Penetration", "Flat Penetration", "Protection Reduction",
    "Physical Damage", "Magical Damage", "Basic Attack", "Ability Damage", "Attack Speed", "Crit", "Burst",
    "Physical Protection", "Magical Protection", "Health", "Mitigation", "Shielding", "Anti-Burst",
    "Aura", "Tenacity", "Slow", "Mana", "Lifesteal", "Healing", "Movement Speed",
    "Tier 3", "Tier 2", "Tier 1",
}

CURATED = {
    # Ratatoskr acorns and god-specific items.
    "Lively Acorn": ["Offense", "Physical Damage", "Tier 2"],
    "Briskberry Acorn": ["Hybrid", "Physical Damage", "Ability Damage", "Crowd Control", "Health", "Sustain", "Tier 3"],
    "Ashwhorl Acorn": ["Hybrid", "Physical Damage", "Attack Speed", "Basic Attack", "Movement Speed", "Health", "Tenacity", "Sustain", "Physical Protection", "Magical Protection", "Tier 3"],
    "Thistlethorn Acorn": ["Hybrid", "Physical Damage", "Ability Damage", "Lifesteal", "Sustain", "Cooldown", "Debuff", "Tier 3"],

    # Core offense and damage items.
    "Ancient Signet": ["Offense", "Magical Damage", "Ability Damage", "Mana", "Cooldown", "Burst", "Tier 3"],
    "Arondight": ["Offense", "Physical Damage", "Cooldown", "Mobility", "Burst", "Utility", "Tier 3"],
    "Avenging Blade": ["Hybrid", "Physical Damage", "Health", "Anti-Tank", "Anti-Protections", "Protection Reduction", "Basic Attack", "Tier 3"],
    "Bancroft's Talon": ["Offense", "Magical Damage", "Lifesteal", "Sustain", "Mana", "Tier 3"],
    "Barbed Carver": ["Offense", "Physical Damage", "Ability Damage", "Lifesteal", "Sustain", "Burst", "Tier 3"],
    "Book of Thoth": ["Offense", "Magical Damage", "Mana", "Flat Penetration", "Scaling", "Tier 3"],
    "Bragi's Harp": ["Offense", "Physical Damage", "Magical Damage", "Basic Attack", "Attack Speed", "Burst", "Tier 3"],
    "Chronos' Pendant": ["Offense", "Magical Damage", "Cooldown", "Ability Damage", "Tier 3"],
    "The Crusher": ["Offense", "Physical Damage", "Ability Damage", "Flat Penetration", "Burst", "Tier 3"],
    "Dagger of Frenzy": ["Offense", "Physical Damage", "Basic Attack", "Attack Speed", "Burst", "Tier 3"],
    "Damaru": ["Offense", "Physical Damage", "Magical Damage", "Cooldown", "Movement Speed", "Mobility", "Ability Damage", "Tier 3"],
    "Death Metal": ["Offense", "Physical Damage", "Magical Damage", "Crit", "Basic Attack", "Burst", "Tier 3"],
    "Deathbringer": ["Offense", "Physical Damage", "Crit", "Basic Attack", "Burst", "Tier 3"],
    "Devourer's Gauntlet": ["Offense", "Physical Damage", "Lifesteal", "Sustain", "Scaling", "Tier 3"],
    "Divine Ruin": ["Offense", "Magical Damage", "Ability Damage", "Flat Penetration", "Anti-Heal", "Tier 3"],
    "Dominance": ["Offense", "Physical Damage", "Basic Attack", "% Penetration", "Anti-Tank", "Anti-Protections", "Mana", "Tier 3"],
    "Doom Orb": ["Offense", "Magical Damage", "Mana", "Movement Speed", "Mobility", "Tier 3"],
    "Dreamer's Idol": ["Offense", "Magical Damage", "Anti-Tank", "Anti-Protections", "% Penetration", "Burst", "Crowd Control", "Tier 3"],
    "Duality": ["Offense", "Physical Damage", "Magical Damage", "Basic Attack", "Attack Speed", "Cooldown", "Tier 3"],
    "Dusk Bringer": ["Offense", "Physical Damage", "Ability Damage", "Cooldown", "Burst", "Tier 3"],
    "Eldritch Dagger": ["Offense", "Physical Damage", "Basic Attack", "Attack Speed", "Flat Penetration", "Tier 3"],
    "Eye of Erebus": ["Offense", "Magical Damage", "Anti-Tank", "Anti-Health", "% Health Damage", "Slow", "Utility", "Tier 3"],
    "Eye of Providence": ["Defense", "Physical Protection", "Magical Protection", "Utility", "Teamfight", "Tier 3"],
    "Gem of Focus": ["Offense", "Magical Damage", "Cooldown", "Movement Speed", "Mobility", "Tier 3"],
    "Gem of Isolation": ["Utility", "Magical Damage", "Ability Damage", "Slow", "Crowd Control", "Health", "Tier 3"],
    "Gluttonous Grimoire": ["Hybrid", "Magical Damage", "Health", "Sustain", "Ability Damage", "Tier 3"],
    "Hastened Fatalis": ["Offense", "Basic Attack", "Attack Speed", "Movement Speed", "Mobility", "Tier 3"],
    "Heartseeker": ["Offense", "Physical Damage", "Ability Damage", "Anti-Tank", "Anti-Health", "% Health Damage", "Tier 3"],
    "Hydra's Lament": ["Offense", "Physical Damage", "Basic Attack", "Ability Damage", "Cooldown", "Burst", "Tier 3"],
    "Jotunn's Revenge": ["Offense", "Physical Damage", "Ability Damage", "Cooldown", "Flat Penetration", "Tier 3"],
    "Killing Stone": ["Offense", "Physical Damage", "Magical Damage", "Burst", "Tier 3"],
    "Jade Scepter": ["Hybrid", "Magical Damage", "Health", "Crowd Control", "Teamfight", "Tier 3"],
    "Lernaean Bow": ["Offense", "Physical Damage", "Basic Attack", "Attack Speed", "Anti-Shield", "Tier 3"],
    "Magi's Shelter": ["Defense", "Magical Protection", "Health", "Crowd Control", "Tenacity", "Anti-Burst", "Tier 3"],
    "Magi's Cloak": ["Defense", "Physical Protection", "Magical Protection", "Crowd Control", "Tenacity", "Anti-Burst", "Tier 3"],
    "Meteor Hammer": ["Hybrid", "Physical Damage", "Magical Damage", "Health", "Ability Damage", "Burst", "Tier 3"],
    "Musashi's Dual Swords": ["Offense", "Physical Damage", "Basic Attack", "Attack Speed", "Movement Speed", "Tier 3"],
    "Nimble Ring": ["Offense", "Magical Damage", "Basic Attack", "Attack Speed", "Lifesteal", "Sustain", "Tier 3"],
    "Obsidian Shard": ["Offense", "Magical Damage", "Anti-Tank", "Anti-Protections", "% Penetration", "Ability Damage", "Tier 3"],
    "Oath-Sworn Spear": ["Offense", "Physical Damage", "Flat Penetration", "Cooldown", "Ability Damage", "Tier 3"],
    "Odyseus' Bow": ["Offense", "Physical Damage", "Basic Attack", "Attack Speed", "Burst", "Tier 3"],
    "Avatar's Parashu": ["Offense", "Physical Damage", "Anti-Tank", "Anti-Protections", "% Penetration", "Burst", "Crowd Control", "Tier 3"],
    "Parashu": ["Offense", "Physical Damage", "Anti-Tank", "Anti-Protections", "% Penetration", "Burst", "Crowd Control", "Tier 3"],
    "Pendulum of the Ages": ["Offense", "Magical Damage", "Cooldown", "Mana", "Ability Damage", "Tier 3"],
    "Phoenix Feather": ["Defense", "Magical Protection", "Health", "Anti-Burst", "Tier 3"],
    "Pendulum Blade": ["Offense", "Physical Damage", "Cooldown", "Mana", "Ability Damage", "Tier 3"],
    "Polynomicon": ["Offense", "Magical Damage", "Basic Attack", "Ability Damage", "Burst", "Mana", "Tier 3"],
    "Qin's Blade": ["Offense", "Physical Damage", "Basic Attack", "Attack Speed", "Anti-Tank", "Anti-Health", "% Health Damage", "Tier 3"],
    "Rage": ["Offense", "Physical Damage", "Crit", "Basic Attack", "Tier 3"],
    "Ragnarok's Wake": ["Defense", "Physical Protection", "Health", "Mobility", "Crowd Control", "Teamfight", "Active", "Tier 3"],
    "Riptalon": ["Offense", "Physical Damage", "Basic Attack", "Lifesteal", "Sustain", "Tier 3"],
    "Rod of Asclepius": ["Hybrid", "Magical Damage", "Healing", "Sustain", "Movement Speed", "Teamfight", "Tier 3"],
    "Rod of Tahuti": ["Offense", "Magical Damage", "Ability Damage", "Burst", "Tier 3"],
    "Sanguine Lash": ["Hybrid", "Physical Damage", "Lifesteal", "Sustain", "Anti-Tank", "Anti-Health", "% Health Damage", "Bruiser", "Tier 3"],
    "Screeching Gargoyle": ["Offense", "Magical Damage", "Ability Damage", "Burst", "Tier 3"],
    "Serrated Edge": ["Offense", "Physical Damage", "Movement Speed", "Lifesteal", "Sustain", "Tier 3"],
    "Shadowdrinker": ["Offense", "Physical Damage", "Flat Penetration", "Mobility", "Burst", "Tier 3"],
    "Silverbranch Bow": ["Offense", "Physical Damage", "Basic Attack", "Attack Speed", "% Penetration", "Anti-Tank", "Anti-Protections", "Tier 3"],
    "Soul Gem": ["Offense", "Magical Damage", "Ability Damage", "Flat Penetration", "Cooldown", "Lifesteal", "Sustain", "Healing", "Tier 3"],
    "Soul Reaver": ["Offense", "Magical Damage", "Ability Damage", "Anti-Tank", "Anti-Health", "% Health Damage", "Tier 3"],
    "Spear of Desolation": ["Offense", "Magical Damage", "Ability Damage", "Flat Penetration", "Cooldown", "Burst", "Tier 3"],
    "Spear Of The Magus": ["Offense", "Magical Damage", "Ability Damage", "% Penetration", "Anti-Tank", "Anti-Protections", "Debuff", "Tier 3"],
    "The Cosmic Horror": ["Offense", "Magical Damage", "Ability Damage", "Flat Penetration", "Burst", "Tier 3"],
    "Staff of Myrddin": ["Offense", "Magical Damage", "Ability Damage", "Cooldown", "Burst", "Tier 3"],
    "Tekko-Kagi": ["Offense", "Physical Damage", "Basic Attack", "Attack Speed", "Flat Penetration", "Mobility", "Tier 3"],
    "The Executioner": ["Offense", "Basic Attack", "Attack Speed", "Anti-Tank", "Anti-Protections", "Protection Reduction", "Physical Damage", "Tier 3"],
    "The Reaper": ["Offense", "Physical Damage", "Flat Penetration", "Lifesteal", "Sustain", "Healing", "Tier 3"],
    "The World Stone": ["Offense", "Magical Damage", "Ability Damage", "Flat Penetration", "Cooldown", "Mana", "Tier 3"],
    "Titan's Bane": ["Offense", "Physical Damage", "Ability Damage", "Anti-Tank", "Anti-Protections", "% Penetration", "Tier 3"],
    "Totem of Death": ["Offense", "Magical Damage", "Ability Damage", "Cooldown", "Anti-Tank", "Anti-Health", "% Health Damage", "Tier 3"],
    "Toxic Blade": ["Offense", "Physical Damage", "Basic Attack", "Attack Speed", "Flat Penetration", "Anti-Heal", "Debuff", "Tier 3"],
    "Transcendence": ["Offense", "Physical Damage", "Mana", "Flat Penetration", "Scaling", "Tier 3"],
    "Tyrfing": ["Offense", "Physical Damage", "Basic Attack", "Attack Speed", "Burst", "Tier 3"],

    # Bruiser, defensive, utility, support items.
    "Alchemist Coat": ["Defense", "Magical Damage", "Health", "Mana", "Mitigation", "Utility", "Tier 3"],
    "Ancile": ["Defense", "Magical Protection", "Health", "Crowd Control", "Anti-Burst", "Tier 3"],
    "Berserker's Shield": ["Hybrid", "Physical Protection", "Health", "Attack Speed", "Basic Attack", "Anti-Burst", "Tier 3"],
    "Bloodforge": ["Offense", "Physical Damage", "Lifesteal", "Sustain", "Shielding", "Cooldown", "Tier 3"],
    "Blood-Bound Book": ["Offense", "Magical Damage", "Lifesteal", "Sustain", "Shielding", "Cooldown", "Tier 3"],
    "Brawler's Beat Stick": ["Hybrid", "Physical Damage", "Magical Damage", "Physical Protection", "Magical Protection", "Anti-Heal", "Bruiser", "Tier 3"],
    "Breastplate of Valor": ["Defense", "Physical Protection", "Cooldown", "Mana", "Anti-Burst", "Tier 3"],
    "Chandra's Grace": ["Defense", "Health", "Cooldown", "Healing", "Sustain", "Teamfight", "Tier 3"],
    "Amanita Charm": ["Defense", "Physical Protection", "Magical Protection", "Health", "Sustain", "Teamfight", "Tier 3"],
    "Circe's Hexstone": ["Defense", "Health", "Cooldown", "Mobility", "Crowd Control", "Anti-Tank", "Anti-Health", "% Health Damage", "Tier 3"],
    "Contagion": ["Defense", "Health", "Crowd Control", "Burst", "Tier 3"],
    "Daybreak Gavel": ["Hybrid", "Physical Damage", "Magical Damage", "Health", "Healing", "Sustain", "Burst", "Tier 3"],
    "Doublet of Binding": ["Defense", "Mitigation", "Teamfight", "Utility", "Tier 3"],
    "Erosion": ["Defense", "Physical Protection", "Magical Protection", "Health", "Anti-Shield", "Anti-Burst", "Tier 3"],
    "Gauntlet of Thebes": ["Defense", "Physical Protection", "Magical Protection", "Health", "Scaling", "Teamfight", "Tier 3"],
    "Gladiator's Shield": ["Hybrid", "Physical Protection", "Health", "Cooldown", "Physical Damage", "Ability Damage", "Bruiser", "Tier 3"],
    "Glorious Pridwen": ["Defense", "Physical Protection", "Magical Protection", "Cooldown", "Shielding", "Burst", "Teamfight", "Tier 3"],
    "Dwarven Plate": ["Defense", "Physical Protection", "Magical Protection", "Anti-Burst", "Tier 3"],
    "Helm of Radiance": ["Defense", "Magical Protection", "Health", "Aura", "Teamfight", "Tier 3"],
    "Helm of Darkness": ["Hybrid", "Magical Damage", "Physical Protection", "Health", "Bruiser", "Tier 3"],
    "Helm of the Phoenix": ["Defense", "Magical Protection", "Health", "Healing", "Sustain", "Tier 3"],
    "Hide of the Nemean Lion": ["Defense", "Physical Protection", "Health", "Anti-Basic Attack", "Anti-Burst", "Tier 3"],
    "Kinetic Cuirass": ["Hybrid", "Physical Protection", "Health", "Physical Damage", "Basic Attack", "Bruiser", "Tier 3"],
    "Leviathan's Hide": ["Defense", "Health", "Physical Protection", "Magical Protection", "Debuff", "Anti-Burst", "Tier 3"],
    "Lifebinder": ["Hybrid", "Magical Damage", "Health", "Healing", "Sustain", "Teamfight", "Tier 3"],
    "Mantle of Discord": ["Defense", "Physical Protection", "Magical Protection", "Tenacity", "Crowd Control", "Anti-Burst", "Tier 3"],
    "Midgardian Mail": ["Defense", "Physical Protection", "Health", "Debuff", "Anti-Basic Attack", "Tier 3"],
    "Mystical Mail": ["Defense", "Physical Protection", "Health", "Aura", "Anti-Shield", "Teamfight", "Tier 3"],
    "Oni Hunter's Garb": ["Defense", "Magical Protection", "Health", "Mitigation", "Anti-Burst", "Tier 3"],
    "Pharaoh's Curse": ["Defense", "Magical Protection", "Health", "Anti-Shield", "Debuff", "Teamfight", "Tier 3"],
    "Prophetic Cloak": ["Defense", "Physical Protection", "Magical Protection", "Mitigation", "Scaling", "Teamfight", "Tier 3"],
    "Regrowth Striders": ["Utility", "Movement Speed", "Mobility", "Healing", "Sustain", "Tier 3"],
    "Resolute Mantle": ["Defense", "Tenacity", "Crowd Control", "Healing", "Sustain", "Scaling", "Tier 3"],
    "Reverent Pridwen": ["Defense", "Physical Protection", "Magical Protection", "Cooldown", "Shielding", "Teamfight", "Tier 3"],
    "Shifter's Shield": ["Hybrid", "Physical Protection", "Magical Protection", "Health", "Physical Damage", "Magical Damage", "Bruiser", "Tier 3"],
    "Shield of the Phoenix": ["Defense", "Physical Protection", "Health", "Cooldown", "Healing", "Sustain", "Mana", "Tier 3"],
    "Shogun's Ofuda": ["Hybrid", "Magical Protection", "Health", "Attack Speed", "Aura", "Teamfight", "Basic Attack", "Tier 3"],
    "Shroud Of Vengeance": ["Defense", "Physical Protection", "Magical Protection", "Tenacity", "Crowd Control", "Physical Damage", "Tier 3"],
    "Spectral Armor": ["Defense", "Physical Protection", "Health", "Anti-Burst", "Crit", "Aura", "Teamfight", "Tier 3"],
    "Sphere of Negation": ["Hybrid", "Magical Damage", "Magical Protection", "Shielding", "Anti-Burst", "Tier 3"],
    "Spirit Robe": ["Defense", "Physical Protection", "Magical Protection", "Tenacity", "Crowd Control", "Healing", "Sustain", "Tier 3"],
    "Stampede": ["Utility", "Physical Protection", "Health", "Movement Speed", "Mobility", "Slow", "Teamfight", "Tier 3"],
    "Stone of Binding": ["Defense", "Physical Protection", "Magical Protection", "Anti-Tank", "Anti-Protections", "Protection Reduction", "Crowd Control", "Teamfight", "Tier 3"],
    "Stygian Anchor": ["Defense", "Physical Protection", "Magical Protection", "Anti-Heal", "Debuff", "Teamfight", "Tier 3"],
    "Triton's Conch": ["Hybrid", "Physical Damage", "Magical Damage", "Health", "Aura", "Teamfight", "Tier 3"],
    "Umbral Link": ["Hybrid", "Physical Protection", "Lifesteal", "Sustain", "Teamfight", "Tier 3"],
    "Vital Amplifier": ["Hybrid", "Physical Damage", "Magical Damage", "Health", "Attack Speed", "Healing", "Sustain", "Basic Attack", "Tier 3"],
    "Void Shield": ["Hybrid", "Physical Protection", "Health", "Physical Damage", "Anti-Tank", "Anti-Protections", "Protection Reduction", "Aura", "Bruiser", "Tier 3"],
    "Void Stone": ["Hybrid", "Magical Protection", "Health", "Magical Damage", "Anti-Tank", "Anti-Protections", "Protection Reduction", "Aura", "Bruiser", "Tier 3"],
    "Wish-Granting Pearl": ["Hybrid", "Magical Damage", "Health", "Mana", "Tier 3"],
    "Wyrmskin Hide": ["Hybrid", "Physical Damage", "Magical Protection", "Health", "Mitigation", "Anti-Burst", "Tier 3"],
    "Xibalban Effigy": ["Defense", "Physical Protection", "Magical Protection", "Mana", "Mitigation", "Anti-Burst", "Tier 3"],
    "Yogi's Necklace": ["Defense", "Health", "Mana", "Healing", "Sustain", "Tier 3"],

    # Starters and starter upgrades.
    "Gilded Arrow": ["Starter", "Offense", "Physical Damage", "Basic Attack", "Attack Speed", "Economy"],
    "Archmage's Gem": ["Starter", "Offense", "Magical Damage", "Physical Damage", "Ability Damage", "Mana", "Burst"],
    "Sharpshooter's Arrow": ["Starter", "Offense", "Physical Damage", "Basic Attack", "Attack Speed", "Crit", "Economy"],
    "Warrior's Axe": ["Starter", "Hybrid", "Physical Protection", "Magical Protection", "Sustain", "Anti-Tank"],
    "Blood-soaked Shroud": ["Starter", "Hybrid", "Health", "Lifesteal", "Sustain", "Ability Damage"],
    "Bluestone Brooch": ["Starter", "Hybrid", "Physical Damage", "Ability Damage", "Health", "Sustain", "Anti-Tank", "Anti-Health", "% Health Damage"],
    "Bluestone Pendant": ["Starter", "Offense", "Physical Damage", "Ability Damage", "Sustain"],
    "Bumba's Cudgel": ["Starter", "Hybrid", "Physical Damage", "Magical Damage", "Health", "Mana", "Cooldown", "Basic Attack"],
    "Bumba's Golden Dagger": ["Starter", "Offense", "Physical Damage", "Magical Damage", "Health", "Basic Attack", "Attack Speed"],
    "Bumba's Hammer": ["Starter", "Hybrid", "Physical Damage", "Magical Damage", "Health", "Cooldown", "Basic Attack", "Healing", "Sustain"],
    "Bumba's Spear": ["Starter", "Offense", "Physical Damage", "Magical Damage", "Health", "Basic Attack", "Attack Speed"],
    "Conduit Gem": ["Starter", "Offense", "Magical Damage", "Ability Damage", "Mana"],
    "Death's Toll": ["Starter", "Hybrid", "Health", "Basic Attack", "Healing", "Sustain", "Mana"],
    "Eros' Bow": ["Utility", "Healing", "Sustain", "Basic Attack", "Tier 3"],
    "Death's Embrace": ["Starter", "Hybrid", "Physical Damage", "Magical Damage", "Basic Attack", "Healing", "Sustain", "Mana", "Cooldown"],
    "Heroism": ["Starter", "Defense", "Physical Protection", "Magical Protection", "Health", "Shielding", "Teamfight", "Economy"],
    "Hunter's Cowl": ["Starter", "Offense", "Basic Attack", "Attack Speed", "Lifesteal", "Sustain"],
    "Leather Cowl": ["Starter", "Offense", "Basic Attack", "Attack Speed", "Lifesteal", "Sustain"],
    "Pendulum of Ages": ["Starter", "Offense", "Magical Damage", "Cooldown", "Mana", "Ability Damage"],
    "Sands of Time": ["Starter", "Offense", "Magical Damage", "Cooldown", "Mana", "Ability Damage"],
    "Selflessness": ["Starter", "Defense", "Physical Protection", "Magical Protection", "Health", "Shielding", "Teamfight"],
    "Sundering Axe": ["Starter", "Hybrid", "Physical Protection", "Magical Protection", "Physical Damage", "Magical Damage", "Healing", "Sustain", "Bruiser"],
    "Sundering Arc": ["Starter", "Utility", "True Damage", "Burst"],
    "Vampiric Shroud": ["Starter", "Hybrid", "Health", "Lifesteal", "Sustain", "Ability Damage", "Mana"],
    "War Banner": ["Starter", "Defense", "Physical Protection", "Magical Protection", "Attack Speed", "Healing", "Sustain", "Movement Speed", "Teamfight"],
    "War Flag": ["Starter", "Defense", "Physical Protection", "Magical Protection", "Attack Speed", "Healing", "Sustain", "Movement Speed", "Teamfight"],

    # Relics and active utility that still appear in metadata.
    "Agility Greaves": ["Utility", "Movement Speed", "Mobility", "Basic Attack", "Tier 3"],
    "Hand of the Abyss": ["Utility", "Crowd Control", "Slow", "Burst", "Tier 3"],
    "Shell of Rebuke": ["Utility", "Defense", "Physical Protection", "Magical Protection", "Shielding", "Mitigation", "Teamfight", "Tier 3"],
    "Talisman of Purification": ["Utility", "Cooldown", "Crowd Control", "Tenacity", "Teamfight", "Tier 3"],

    # Tier 1/2 components and simple pieces.
    "Adamantine Sickle": ["Offense", "Physical Damage", "Lifesteal", "Sustain", "Tier 2"],
    "Adroit Ring": ["Utility", "Cooldown", "Tier 2"],
    "Axe": ["Offense", "Physical Damage", "Tier 1"],
    "Battle Axe": ["Hybrid", "Physical Damage", "Health", "Tier 2"],
    "Bow": ["Offense", "Basic Attack", "Attack Speed", "Tier 1"],
    "Bowl Drum": ["Offense", "Ability Damage", "Tier 2"],
    "Caestus": ["Hybrid", "Physical Damage", "Cooldown", "Physical Protection", "Magical Protection", "Tier 2"],
    "Captain's Ring": ["Defense", "Physical Protection", "Cooldown", "Tier 2"],
    "Circlet": ["Utility", "Mana", "Tier 1"],
    "Cursed Sickle": ["Offense", "Lifesteal", "Sustain", "Tier 2"],
    "Devours Gloves": ["Offense", "Physical Damage", "Lifesteal", "Sustain", "Tier 2"],
    "Demonic Grip": ["Offense", "Magical Damage", "Attack Speed", "Anti-Tank", "Anti-Protections", "Protection Reduction", "Tier 2"],
    "Gem": ["Offense", "Magical Damage", "Mana", "Tier 1"],
    "Glowing Emerald": ["Defense", "Health", "Tier 1"],
    "Heavy Hammer": ["Hybrid", "Physical Damage", "Health", "Tier 2"],
    "Imperial Helmet": ["Defense", "Physical Protection", "Health", "Tier 2"],
    "Magic Focus": ["Offense", "Magical Damage", "Flat Penetration", "Tier 2"],
    "Mace": ["Offense", "Physical Damage", "Flat Penetration", "Tier 2"],
    "Moonstone": ["Offense", "Magical Damage", "Mana", "Tier 2"],
    "Morningstar": ["Offense", "Physical Damage", "Mana", "Tier 2"],
    "Mystic Robe": ["Defense", "Magical Protection", "Health", "Tier 2"],
    "Ring": ["Offense", "Magical Damage", "Attack Speed", "Tier 2"],
    "Round Shield": ["Defense", "Physical Protection", "Tier 2"],
    "Sash": ["Hybrid", "Physical Damage", "Magical Damage", "Tier 2"],
    "Scythe": ["Offense", "Physical Damage", "Lifesteal", "Sustain", "Tier 2"],
    "Shield": ["Defense", "Physical Protection", "Tier 1"],
    "Freya's Tears": ["Utility", "Tier 3"],
    "Skeggox": ["Offense", "Physical Damage", "Tier 2"],
    "Soul Reliquary": ["Offense", "Magical Damage", "Mana", "Tier 2"],
    "Survivor's Sash": ["Hybrid", "Physical Damage", "Magical Damage", "Tier 2"],
    "Veve Charm": ["Defense", "Health", "Tier 2"],
    "Void Shard": ["Offense", "Magical Damage", "% Penetration", "Anti-Tank", "Anti-Protections", "Tier 2"],
    "Zither": ["Offense", "Physical Damage", "Magical Damage", "Tier 2"],
}

ALIASES = {
    "Qins Blade": "Qin's Blade",
    "Bumbas Cudgel": "Bumba's Cudgel",
    "Bumbas Dagger": "Bumba's Golden Dagger",
    "Bumbas Hammer": "Bumba's Hammer",
    "Bumbas Spear": "Bumba's Spear",
    "Bancrofts Talon": "Bancroft's Talon",
    "Chronos Pendant": "Chronos' Pendant",
    "Jotunns Revenge": "Jotunn's Revenge",
    "Oni Hunters Garb": "Oni Hunter's Garb",
    "Shifters Shield": "Shifter's Shield",
    "Shoguns Kusari": "Shogun's Ofuda",
    "Shogun's Kusari": "Shogun's Ofuda",
    "Titans Bane": "Titan's Bane",
    "Bindings Of Lyngvi": "Stone of Binding",
    "Bindings of Lyngvi": "Stone of Binding",
    "Shape Shifter Shield": "Shifter's Shield",
    "Gladiator Shield": "Gladiator's Shield",
    "Dwarf Forged Plate Physical": "Dwarven Plate",
    "Restorative Amanita": "Amanita Charm",
    "Design Temp T3 New Sun Beam Bow": "Sun Beam Bow",
    "Utility Purification Beads": "Purification Beads",
    "Spearofthe Magus": "Spear Of The Magus",
    "Staff of Cosmic Horror": "The Cosmic Horror",
    "Xibalban Effigy New": "Xibalban Effigy",
    "Erosbow": "Eros' Bow",
    "Eros' Bow": "Eros' Bow",
}

def norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())

def tags_from_stats_type(row):
    name = row.get("name") or "Unknown Item"
    item_type = str(row.get("type") or "")
    stats = " ".join(row.get("stats") or [])
    cats = row.get("categoriesSeen") or []
    tags = []
    if "Starter" in item_type or "Starter" in cats: tags.append("Starter")
    if "Tier 3" in item_type or "Tier 3" in cats: tags.append("Tier 3")
    if "Tier 2" in item_type or "Tier 2" in cats: tags.append("Tier 2")
    if "Tier 1" in item_type or "Tier 1" in cats: tags.append("Tier 1")
    if "Offensive" in item_type: tags.append("Offense")
    if "Defensive" in item_type: tags.append("Defense")
    if "Hybrid" in item_type: tags.append("Hybrid")
    if re.search(r"Strength|Inhand Power|Physical", stats, re.I): tags.append("Physical Damage")
    if re.search(r"Intelligence|Magical", stats, re.I): tags.append("Magical Damage")
    if re.search(r"Physical Protection", stats, re.I): tags.extend(["Defense", "Physical Protection"])
    if re.search(r"Magical Protection", stats, re.I): tags.extend(["Defense", "Magical Protection"])
    if re.search(r"Max Health|Health Regen", stats, re.I): tags.append("Health")
    if re.search(r"Max Mana|Mana Regen", stats, re.I): tags.append("Mana")
    if re.search(r"Cooldown", stats, re.I): tags.append("Cooldown")
    if re.search(r"Tenacity|Crowd Control Reduction|CCR", stats, re.I): tags.extend(["Tenacity", "Crowd Control"])
    if re.search(r"Attack Speed", stats, re.I): tags.extend(["Basic Attack", "Attack Speed"])
    if re.search(r"Crit", stats, re.I): tags.extend(["Basic Attack", "Crit"])
    if re.search(r"Lifesteal", stats, re.I): tags.extend(["Lifesteal", "Sustain"])
    if re.search(r"Penetration 10%|Penetration 20%", stats, re.I): tags.extend(["Anti-Tank", "Anti-Protections", "% Penetration"])
    elif re.search(r"Penetration", stats, re.I): tags.extend(["Anti-Tank", "Anti-Protections", "Flat Penetration"])
    return tags

def unique(values):
    output = []
    for value in values:
        if value and value not in output:
            output.append(value)
    return output

HIDDEN_ITEM_NAMES = {
    "Aegis of Acceleration", "Agility Relic", "Blink Rune", "Buff Battle Cry",
    "Curios Gjallarflare", "Design Temp T3 New Sun Beam Bow", "Dwarf Forged Plate Physical",
    "Gjallarflare", "Influences Sunder", "Influences Sundering Echo", "Phantom Shell",
    "Restorative Amanita", "Time Lock Aegis", "Utility Purification Beads",
    "t Blink 128", "t Blink Abyss 128", "shieldsplitter", "Curseweaver T3 Tweak", "Zisurru",
}

DEFAULT_OUTPUT = Path("data/item_taxonomy.json")


def metadata_fingerprint(row):
    """Build a stable patch-watch hash from the fields that change item meaning."""
    payload = {
        "displayName": row.get("displayName") or row.get("name") or "",
        "itemType": row.get("itemType") or row.get("item_type") or "",
        "stats": row.get("stats") or [],
        "passive": row.get("passive") or "",
        "categoriesSeen": row.get("categoriesSeen") or row.get("categories_seen") or [],
        "cost": row.get("cost"),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def load_current_item_rows():
    """Return one best metadata row per canonical item name from Supabase/local metadata."""
    rows_by_name = {}
    for row in app.load_item_metadata_rows():
        name = row.get("displayName") or row.get("name")
        if not name or name in HIDDEN_ITEM_NAMES:
            continue
        canonical = ALIASES.get(name, name)
        current = rows_by_name.get(canonical)
        if not current or app.item_metadata_score(row) > app.item_metadata_score(current):
            rows_by_name[canonical] = row
    return rows_by_name


def build_taxonomy():
    """Create the reviewed taxonomy payload consumed by the app."""
    items = {}
    rows_by_name = load_current_item_rows()
    for canonical, row in rows_by_name.items():
        source_name = row.get("displayName") or row.get("name") or canonical
        name = canonical
        curated_tags = CURATED.get(canonical) or CURATED.get(source_name)
        if not curated_tags:
            curated_tags = tags_from_stats_type({
                "name": canonical,
                "type": row.get("itemType") or row.get("item_type") or "",
                "stats": row.get("stats") or [],
                "categoriesSeen": row.get("categoriesSeen") or row.get("categories_seen") or [],
            })
        tags = unique(curated_tags)
        beginner = "Starter" if "Starter" in tags else "Hybrid" if "Hybrid" in tags else "Defense" if "Defense" in tags else "Offense" if "Offense" in tags else "Utility" if "Utility" in tags else "Utility"
        items[canonical] = {
            "name": canonical,
            "beginnerType": beginner,
            "tags": tags,
            "reviewed": bool(CURATED.get(canonical) or CURATED.get(name)),
            "metadataFingerprint": metadata_fingerprint(row),
            "sourceName": source_name if source_name != canonical else "",
        }


    return {
        "version": 1,
        "notes": "Curated item taxonomy for High Council Hub. Tags are intentionally build-purpose oriented, not broad keyword matches.",
        "items": dict(sorted(items.items(), key=lambda pair: pair[0].lower())),
    }


def audit_taxonomy(output_path):
    """Compare the stored taxonomy against the latest item metadata after a patch."""
    if not output_path.exists():
        current = build_taxonomy()
        current_items = current.get("items", {})
        return {
            "ok": True,
            "needsReview": True,
            "reason": f"{output_path} does not exist yet.",
            "itemsInMetadata": len([item for item in current_items.values() if not item.get("aliasOf")]),
            "itemsInTaxonomy": 0,
            "newItems": sorted(name for name, item in current_items.items() if not item.get("aliasOf")),
            "removedItems": [],
            "changedItems": [],
            "missingFingerprints": [],
            "unreviewedItems": sorted(name for name, item in current_items.items() if not item.get("reviewed") and not item.get("aliasOf")),
        }

    stored = json.loads(output_path.read_text())
    current = build_taxonomy()
    stored_items = stored.get("items", {})
    current_items = current.get("items", {})
    stored_names = {name for name, item in stored_items.items() if not item.get("aliasOf")}
    current_names = {name for name, item in current_items.items() if not item.get("aliasOf")}

    missing_fingerprints = sorted(
        name for name in stored_names & current_names if not stored_items.get(name, {}).get("metadataFingerprint")
    )
    changed_items = sorted(
        name for name in stored_names & current_names
        if stored_items.get(name, {}).get("metadataFingerprint")
        and stored_items[name].get("metadataFingerprint") != current_items.get(name, {}).get("metadataFingerprint")
    )
    unreviewed_items = sorted(
        name for name, item in stored_items.items() if not item.get("reviewed") and not item.get("aliasOf")
    )

    summary = {
        "ok": True,
        "needsReview": bool((current_names - stored_names) or (stored_names - current_names) or changed_items or missing_fingerprints),
        "itemsInMetadata": len(current_names),
        "itemsInTaxonomy": len(stored_names),
        "newItems": sorted(current_names - stored_names),
        "removedItems": sorted(stored_names - current_names),
        "changedItems": changed_items,
        "missingFingerprints": missing_fingerprints,
        "unreviewedItems": unreviewed_items,
    }
    return summary


def write_taxonomy(output_path):
    """Persist the latest taxonomy and print a compact build summary."""
    output = build_taxonomy()
    output_path.write_text(json.dumps(output, indent=2) + "\n")
    items = output.get("items", {})
    return {
        "ok": True,
        "output": str(output_path),
        "items": len(items),
        "reviewed": sum(1 for item in items.values() if item.get("reviewed")),
        "unreviewed": [name for name, item in items.items() if not item.get("reviewed") and not item.get("aliasOf")][:50],
    }


def main():
    parser = argparse.ArgumentParser(description="Build or audit the High Council Hub item taxonomy.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Path to the taxonomy JSON file.")
    parser.add_argument("--audit", action="store_true", help="Report item taxonomy drift without writing files.")
    parser.add_argument("--fail-on-changes", action="store_true", help="Exit with code 1 when the audit finds review work.")
    args = parser.parse_args()

    output_path = Path(args.output)
    if args.audit:
        summary = audit_taxonomy(output_path)
        print(json.dumps(summary, indent=2))
        if args.fail_on_changes and summary.get("needsReview"):
            raise SystemExit(1)
        return

    print(json.dumps(write_taxonomy(output_path), indent=2))


if __name__ == "__main__":
    main()

