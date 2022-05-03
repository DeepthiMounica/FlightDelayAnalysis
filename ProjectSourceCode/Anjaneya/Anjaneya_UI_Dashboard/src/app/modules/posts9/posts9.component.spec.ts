import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Posts9Component } from './posts9.component';

describe('Posts9Component', () => {
  let component: Posts9Component;
  let fixture: ComponentFixture<Posts9Component>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ Posts9Component ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(Posts9Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
