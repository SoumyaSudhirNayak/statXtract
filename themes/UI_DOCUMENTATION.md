# STATXTRACT UI Technical Documentation

This document provides a granular, end-to-end technical breakdown of the **STATXTRACT** UI/UX. It details the design systems, component specifications, and interaction logic that create the platform's high-end desktop experience.

---

## 1. Global Design System

### 1.1 Typography
- **Header Font (`var(--font-head)`)**: `Poppins`, sans-serif.
  - Used for all branding, page titles, and card headers.
  - **Weights**: 900 (Black), 800 (ExtraBold), 700 (Bold).
  - **Scaling**:
    - **Splash Logo**: `120px`
    - **Page Hero Titles**: `text-6xl`
    - **Section Headers**: `text-4xl`
    - **Card Titles**: `text-3xl`
- **Body Font**: `Inter`, sans-serif.
  - Used for descriptions, labels, and data tables.
  - **Weights**: 400 (Regular), 500 (Medium), 700 (Bold).
  - **Scaling**:
    - **Standard Body**: `text-xl`
    - **Secondary Info**: `text-lg`
    - **Small Labels**: `text-sm` (always uppercase + tracking-widest).

### 1.2 Color Architecture
| Module | Primary Color | Shadow/Glow | Usage |
| :--- | :--- | :--- | :--- |
| **Global Header** | `#1F3A5F` (Navy) | `shadow-xl` | Main App Header, Navigation Background |
| **Admin Module** | `#2E5BBA` (Royal Blue) | `rgba(46, 91, 186, 0.25)` | Admin Cards, Sidebar Active, Progress Bars |
| **User Module** | `#2E7D32` (Forest Green) | `rgba(46, 125, 50, 0.25)` | User Dashboard, Success Actions |
| **Student** | `#D32F2F` (Red) | `rgba(211, 47, 47, 0.25)` | Student Verification & Cards |
| **Researcher** | `#1976D2` (Blue) | `rgba(25, 118, 210, 0.25)` | Researcher Verification & Cards |
| **Private Org** | `#7B1FA2` (Purple) | `rgba(123, 31, 162, 0.25)` | Private Sector Verification & Cards |
| **Analyst** | `#F57C00` (Orange) | `rgba(245, 124, 0, 0.25)` | Analyst Verification & Cards |
| **Neutral Border** | `#D1D5DB` (Silver) | N/A | Default state for all cards and inputs |

### 1.3 Global Animations
- **`lift-on-hover`**: `transition: transform 0.2s cubic-bezier(0.4, 0, 0.2, 1)`. Moves element `-4px` on Y-axis.
- **`AnimatedBackground`**: Slow-moving SVG paths with low-opacity fills (`opacity: 0.1`) that float across the screen to create depth.
- **Splash Pulse**: Pulsating text shadow using Framer Motion: `text-shadow: [0 10px 40px rgba(31,58,95,0.2), 0 10px 60px rgba(46,91,186,0.4)]`.

---

## 2. Component Breakdown

### 2.1 The Header System
- **App Header (`Header.tsx`)**:
  - **Height/Padding**: `px-10 py-6`.
  - **Branding**: `STATXTRACT` in white, `text-4xl`, `font-black`.
  - **Profile Button**: Royal Blue (`#2E5BBA`) for Admin, Forest Green (`#2E7D32`) for User.
  - **Style**: `bg-[#1F3A5F]`, `shadow-xl`, `z-50`.
- **Navigation Bar (`Navigation.tsx`)**:
  - White background, `border-b-2 border-[#2E5BBA]`.
  - Breadcrumbs use `Home` icon and `ChevronRight`.
  - Active crumb: `text-[#2E5BBA]`, `bg-[#2E5BBA]/5`, `border-2 border-[#2E5BBA]`, `px-4 py-2`.

### 2.2 The Card Ecosystem
All cards share a base of `bg-white/80`, `backdrop-blur-sm`, `border-2`, and `rounded-2xl`.

#### A. Portal Cards (`ModuleSelection.tsx`)
- **Size**: `min-h-[450px]`, `p-12`.
- **Hover State**: Border changes to module color (Blue/Green), Shadow becomes themed.
- **Icon Box**: `28x28` rounded-3xl box. Scales `1.1x` and rotates `5deg` on hover.

#### B. Auth Cards (`AdminAuth.tsx`)
- **Size**: `min-h-[400px]`, `p-12`.
- **Design**: Centered layout, large `w-28` icon box with gradient backgrounds.
- **Interaction**: Themed glow matching the action (Blue for Sign In, Green for Create Account).

#### C. Organization Cards (`OrganizationTypeSelection.tsx`)
- **Size**: `min-h-[320px]`, `p-12`.
- **Unique Feature**: A low-opacity (`opacity-10`) circular accent in the bottom-right corner that scales `1.5x` on hover.
- **Colors**: Dynamic coloring based on the org type (Red, Blue, Purple, Orange).

#### D. Dashboard Cards (`DashboardCard.tsx`)
- **Size**: `min-h-[300px]`, `p-10`.
- **Design**: Icon box in top-left, `ArrowRight` icon in top-right.
- **Hover**: Icon box background changes to theme color, icon color changes to white.

#### E. Pricing Cards (`UserDashboard.tsx`)
- **Layout**: 3-column grid, `rounded-3xl`.
- **Premium (Most Popular)**: Royal Blue header, `scale-[1.02]`, Crown icon, elevated shadow.
- **Features**: Checklist using `Check` icon from Lucide, `₹` currency symbol in massive `text-6xl`.

### 2.3 Sidebars (User Module)
- **States**: 
  - **Expanded**: `w-80` (320px), shows icons + labels.
  - **Collapsed**: `w-24` (96px), shows large `w-10 h-10` icons only.
- **Active Item**: `bg-[#2E7D32]` (Green), `text-white`, `rounded-xl`, `shadow-lg`.
- **Hover Item**: `bg-gray-100`, `text-[#2E7D32]`.

### 2.4 Form & Input System
- **Input Fields**:
  - `bg-white`, `border-2 border-[#D1D5DB]`, `rounded-xl`, `px-6 py-4`, `text-xl`.
  - **Hover/Focus**: Border changes to theme color, `box-shadow` matching the theme color appears.
- **Select Dropdowns**: Same styling as inputs, including the themed glow on hover.
- **Buttons**:
  - `py-5`, `rounded-xl`, `text-2xl`, `font-black`.
  - Themed background with white text.
  - Hover: Border changes to Saffron (`#F4A300`), shadow becomes high-intensity.

---

## 3. Data Visualization & Tables
- **Tables**:
  - Header: `bg-[#1F3A5F]`, white text, `font-bold`, `uppercase`, `text-lg`.
  - Rows: `px-8 py-6`, `text-xl`.
  - Hover: `bg-[#2E5BBA]/5` (5% opacity Royal Blue).
- **Charts (`Recharts`)**:
  - **BarChart**: Radius `[8, 8, 0, 0]` on bars, Royal Blue fill.
  - **PieChart**: Inner radius `80`, Outer radius `110`, themed colors.
  - **Tooltips**: `rounded-xl`, `border-2 border-[#D1D5DB]`, `text-lg`.

---
*Document Version: 2.0 (End-to-End Granular Detail)*
# STATXTRACT: The Ultimate UI/UX Master Blueprint

This document is a highly detailed guide explaining exactly how the **STATXTRACT** interface was designed and built. It is written so that anyone—whether a designer, developer, or a curious observer—can understand the "magic" behind the professional, high-end desktop experience.

---

## 1. What does the "Link Syntax" mean?
You might see links like `UI_DOCUMENTATION.md#L116-117`. 
- **The Path**: `c:\...\UI_DOCUMENTATION.md` is the location of the file on the computer.
- **The Anchor**: `#L116-117` is a "line range". It tells the computer to jump directly to **Line 116** and highlight everything until **Line 117**. It's a quick way to point to a specific sentence or piece of code.

---

## 2. The "Bigger & Bolder" Philosophy
Unlike standard websites that feel cramped, STATXTRACT uses a **"Maximized Desktop"** strategy. 
- **Why?** To make the data feel important and easy to read from a distance.
- **How?** By using massive containers (up to `1800px` wide), huge padding (`p-12` or `48px` of space inside boxes), and giant font sizes.

---

## 3. Typography: The Power of Fonts
We use two specific fonts that work together like a team:

### 3.1 Poppins (The Attention-Grabber)
- **Role**: Used for everything that needs to "pop"—Titles, Branding, and Card Headers.
- **Style**: We use the "Black" weight (900), which is the thickest possible version of the font.
- **Impact**: It makes the brand "STATXTRACT" look strong and unshakeable.
- **Giant Sizes**: 
  - **Main Titles**: `text-6xl` (approx. `60px` tall).
  - **Branding**: `text-4xl` (approx. `36px` tall) with `tracking-tighter` (letters are squeezed closer for a modern look).

### 3.2 Inter (The Reliable Reader)
- **Role**: Used for descriptions, labels, and the actual data in tables.
- **Style**: Clean, modern, and extremely easy to read.
- **Impact**: Even when there is a lot of data, the eyes don't get tired because the font is so clear.

---

## 4. The Color Language
Colors aren't just for decoration; they tell the user where they are.

| Color | Hex Code | Visual Meaning |
| :--- | :--- | :--- |
| **Navy Blue** | `#1F3A5F` | **Authority**: Used in the main top bar to show this is a secure, official platform. |
| **Royal Blue** | `#2E5BBA` | **Action (Admin)**: The color of the "Admin" side. It feels technical and precise. |
| **Forest Green** | `#2E7D32` | **Growth (User)**: The color of the "User" side. It feels safe, welcoming, and successful. |
| **Saffron Orange** | `#F4A300` | **Energy**: Used for hover glows and important highlights to catch the eye instantly. |

---

## 5. Every Detail of the "Cards" (The Boxes)
Every "box" or "card" in the app follows a strict 3-step design rule to feel "Premium":

1. **The Glass Effect**: Every card is slightly transparent (`bg-white/80`) and blurs the background behind it (`backdrop-blur-sm`). This makes it look like high-end frosted glass.
2. **The Default State**: A simple, thin silver border (`#D1D5DB`) and no shadow. This keeps the screen looking clean and uncluttered.
3. **The Hover Magic (The "Glow")**: When you move your mouse over a card:
   - **Movement**: The card physically lifts up by `4px` (`lift-on-hover`).
   - **Border Change**: The silver border disappears and is replaced by a bright themed color (Blue, Green, etc.).
   - **The Glow Shadow**: A soft, colored glow spreads out from behind the card, making it look like it's glowing from underneath.

---

## 6. The Header & Navigation Details
### 6.1 The Main Header (Top Bar)
- **Color**: Solid Navy (`#1F3A5F`).
- **White Branding**: The word "STATXTRACT" is kept pure white to contrast against the dark navy.
- **Shadow**: A "shadow-xl" is used to make the header look like it is floating above the rest of the page.

### 6.2 The Sidebar (The Control Panel)
- **When Open**: It's a wide panel (`320px`) with large, bold text labels.
- **When Closed**: It shrinks to a slim bar (`96px`).
- **The Icons**: Instead of boring text, we show large, colorful icons (like a Crown for Pricing or a Search icon for Query). These icons are much bigger than normal (`w-10 h-10`) so they are easy to see.

---

## 7. Animations: Bringing the UI to Life
- **Floating Background**: If you look closely at the background, you'll see soft, colorful shapes drifting slowly. This is the `AnimatedBackground`. It's designed to be so subtle that you almost don't notice it, but it makes the app feel "alive".
- **Splash Pulse**: When the app first loads, the logo doesn't just sit there—it "pulses" with a glowing shadow that grows and shrinks, like a heartbeat.

---

## 8. Summary of Sizes (For the "Bigger" Look)
- **Buttons**: `py-5` (very tall buttons) with `text-2xl` (huge font). They feel very "clickable".
- **Padding**: We use `p-12` (48 pixels of space) around almost everything. This gives the content "room to breathe".
- **Rounded Corners**: We use `rounded-2xl` (16px) or `rounded-3xl` (24px). Sharp corners look "old"; round corners look "modern and friendly".

---
# STATXTRACT: The Ultimate UI/UX Master Blueprint

This document is a highly detailed guide explaining exactly how the **STATXTRACT** interface was designed and built. It is written so that anyone—whether a designer, developer, or a curious observer—can understand the "magic" behind the professional, high-end desktop experience.

---

## 1. What does the "Link Syntax" mean?
You might see links like `UI_DOCUMENTATION.md#L116-117`. 
- **The Path**: `c:\...\UI_DOCUMENTATION.md` is the location of the file on the computer.
- **The Anchor**: `#L116-117` is a "line range". It tells the computer to jump directly to **Line 116** and highlight everything until **Line 117**. It's a quick way to point to a specific sentence or piece of code.

---

## 2. Screen-by-Screen Journey

### **Screen 1: The Splash Screen (The Entrance)**
*This is the first thing you see when the app loads.*
- **The UI**:
  - **Background**: Solid Deep Navy (`#1F3A5F`).
  - **Logo**: A massive `120px` tall "STATXTRACT" in pure white.
  - **Animation**: The logo doesn't just appear; it fades in and scales up. It also has a "heartbeat" pulse effect where its shadow grows and shrinks every few seconds.
- **What happens next?**: After 2.5 seconds, the app automatically "slides" you into the Module Selection screen.

### **Screen 2: Portal Selection (The Choice)**
*Here, you choose whether you are an Admin or a User.*
- **The UI**:
  - **The Cards**: Two giant vertical boxes (`450px` tall). 
  - **The Icons**: Large icons inside rounded-square boxes that "tilt" and "grow" when you touch them with your mouse.
  - **The Colors**: 
    - **Admin** uses Royal Blue (`#2E5BBA`).
    - **User** uses Forest Green (`#2E7D32`).
- **The Interaction**:
  - Click **Admin** -> Takes you to the Admin Access screen.
  - Click **User** -> Takes you to the User Portal screen.

---

### **Screen 3: Admin Module Flow**

#### **Step A: Admin Access (`AdminAuth`)**
- **The UI**: Two clean, centered cards: "Sign In" and "Create Account".
- **The Interaction**:
  - Click **Sign In** -> Takes you to the Email/Password screen.
  - Click **Create Account** -> Takes you to the Admin Registration form.

#### **Step B: Admin Sign In (`AdminSignIn`)**
- **The UI**: A focused form box (`800px` wide).
  - **Inputs**: Large boxes with bold text.
  - **Hover Effect**: When you click inside an input, the border turns Blue and a soft Blue glow appears around it.
- **The Interaction**: Clicking "Sign In" logs you in and opens the **Admin Dashboard**.

#### **Step C: Admin Dashboard**
- **The UI**: A wide-screen grid of "Feature Cards" (Upload Dataset, Usage Logs, System Settings, etc.).
  - **The Header**: A Navy bar with "STATXTRACT" in white.
  - **The Navigation**: A thin bar below the header showing "Home > Dashboard" so you never get lost.
- **The Interaction**: Clicking any card (like "Usage Logs") opens a full-screen table with all the data.

---

### **Screen 4: User Module Flow**

#### **Step A: User Portal Selection**
- **The UI**: Similar to Admin, offering "Sign In" or "Create Account".

#### **Step B: Create Account (The 3-Step Process)**
1. **Step 1 (Basic Info)**: You enter your name and email.
2. **Step 2 (Organization Type)**: Four colorful cards appear (Student, Researcher, Private, Analyst).
   - **Student (Red)**: Card glows red on hover.
   - **Researcher (Blue)**: Card glows blue on hover.
   - **The Scaling Accent**: Each card has a faint circle in the corner that grows `1.5x` bigger when you hover.
3. **Step 3 (Verification)**: A massive table appears where you upload your ID or credentials. Everything is scaled up to be "Big and Bold."

#### **Step C: User Dashboard**
- **The UI**: 
  - **Sidebar**: A slim panel on the left with large icons. If you open it, it slides out to show text labels.
  - **Main Area**: Shows your recent activity and a "Fetch Data" section.
- **The Interaction**: 
  - Click **Query** -> Opens the data tool where you can see tables and charts side-by-side.
  - Click **Plans** -> Opens the premium pricing grid with Free, Basic, and Premium tiers.

---

## 3. The "Bigger & Bolder" Philosophy
Unlike standard websites that feel cramped, STATXTRACT uses a **"Maximized Desktop"** strategy. 
- **Why?** To make the data feel important and easy to read from a distance.
- **How?** By using massive containers (up to `1800px` wide), huge padding (`p-12` or `48px` of space inside boxes), and giant font sizes.

---

## 4. Typography: The Power of Fonts
We use two specific fonts that work together like a team:

### 4.1 Poppins (The Attention-Grabber)
- **Role**: Used for everything that needs to "pop"—Titles, Branding, and Card Headers.
- **Style**: We use the "Black" weight (900), which is the thickest possible version of the font.
- **Impact**: It makes the brand "STATXTRACT" look strong and unshakeable.

### 4.2 Inter (The Reliable Reader)
- **Role**: Used for descriptions, labels, and the actual data in tables.
- **Style**: Clean, modern, and extremely easy to read.

---

## 5. Summary of Animations
- **Floating Background**: Soft, colorful shapes drifting slowly in the background (`AnimatedBackground`).
- **Lift-on-Hover**: Every card physically moves up by `4px` when you hover over it, making the UI feel "physical."
- **The Glow**:Programmatic transitions from Gray borders to themed color glows.

---
*Document Version: 4.0 (End-to-End Screen Breakdown)*
